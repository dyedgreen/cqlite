use super::plan::{Filter, LoadProperty, MatchStep, QueryPlan, UpdateStep};
use crate::Error;
use crate::{parser::ast, Property};
use std::collections::HashMap;

pub(crate) struct BuildEnv<'src> {
    names: HashMap<&'src str, NamedEntity>,
    next_name: usize,
}

#[derive(Debug, Clone, Copy)]
enum NamedEntity {
    Node(usize),
    Edge(usize),
}

impl<'src> BuildEnv<'src> {
    fn new() -> Self {
        Self {
            names: HashMap::new(),
            next_name: 0,
        }
    }

    fn next_name(&mut self) -> usize {
        self.next_name += 1;
        self.next_name - 1
    }

    fn get_node(&self, name: &str) -> Result<Option<usize>, Error> {
        match self.names.get(&name) {
            Some(NamedEntity::Node(name)) => Ok(Some(*name)),
            Some(NamedEntity::Edge(_)) => Err(Error::Todo),
            None => Ok(None),
        }
    }

    fn get_edge(&self, name: &str) -> Result<Option<usize>, Error> {
        match self.names.get(&name) {
            Some(NamedEntity::Node(_)) => Err(Error::Todo),
            Some(NamedEntity::Edge(name)) => Ok(Some(*name)),
            None => Ok(None),
        }
    }

    fn create_node(&mut self, name: &'src str) -> Result<usize, Error> {
        match self.names.get(&name) {
            Some(NamedEntity::Node(name)) => Ok(*name),
            Some(NamedEntity::Edge(_)) => Err(Error::Todo),
            None => {
                let next_name = self.next_name();
                self.names.insert(name, NamedEntity::Node(next_name));
                Ok(next_name)
            }
        }
    }

    fn create_edge(&mut self, name: &'src str) -> Result<usize, Error> {
        match self.names.get(&name) {
            Some(NamedEntity::Node(_)) => Err(Error::Todo),
            Some(NamedEntity::Edge(name)) => Ok(*name),
            None => {
                let next_name = self.next_name();
                self.names.insert(name, NamedEntity::Edge(next_name));
                Ok(next_name)
            }
        }
    }

    fn build_load_property(&mut self, expr: &ast::Expression) -> Result<LoadProperty, Error> {
        let load = match expr {
            ast::Expression::Parameter(name) => LoadProperty::Parameter {
                name: name.to_string(),
            },
            ast::Expression::Literal(literal) => LoadProperty::Constant(match literal {
                ast::Literal::Integer(i) => Property::Integer(*i),
                ast::Literal::Real(r) => Property::Real(*r),
                ast::Literal::Boolean(b) => Property::Boolean(*b),
                ast::Literal::Text(t) => Property::Text(t.to_string()),
                ast::Literal::Null => Property::Null,
            }),
            ast::Expression::IdOf { name } => match self.names.get(name).ok_or(Error::Todo)? {
                NamedEntity::Node(node) => LoadProperty::IdOfNode { node: *node },
                NamedEntity::Edge(edge) => LoadProperty::IdOfEdge { edge: *edge },
            },
            ast::Expression::Property { name, key } => {
                match self.names.get(name).ok_or(Error::Todo)? {
                    NamedEntity::Node(node) => LoadProperty::PropertyOfNode {
                        node: *node,
                        key: key.to_string(),
                    },
                    NamedEntity::Edge(edge) => LoadProperty::PropertyOfEdge {
                        edge: *edge,
                        key: key.to_string(),
                    },
                }
            }
        };
        Ok(load)
    }

    fn build_filter(&mut self, cond: &ast::Condition) -> Result<Filter, Error> {
        let filter = match cond {
            ast::Condition::And(a, b) => Filter::and(self.build_filter(a)?, self.build_filter(b)?),
            ast::Condition::Or(a, b) => Filter::or(self.build_filter(a)?, self.build_filter(b)?),
            ast::Condition::Not(inner) => Filter::not(self.build_filter(inner)?),

            ast::Condition::Expression(expr) => Filter::IsTruthy(self.build_load_property(expr)?),

            ast::Condition::Eq(a, b) => {
                Filter::Eq(self.build_load_property(a)?, self.build_load_property(b)?)
            }
            ast::Condition::Ne(a, b) => Filter::not(Filter::Eq(
                self.build_load_property(a)?,
                self.build_load_property(b)?,
            )),

            ast::Condition::Lt(a, b) => {
                Filter::Lt(self.build_load_property(a)?, self.build_load_property(b)?)
            }
            ast::Condition::Le(a, b) => Filter::or(
                Filter::Lt(self.build_load_property(a)?, self.build_load_property(b)?),
                Filter::Eq(self.build_load_property(a)?, self.build_load_property(b)?),
            ),

            ast::Condition::Gt(a, b) => {
                Filter::Gt(self.build_load_property(a)?, self.build_load_property(b)?)
            }
            ast::Condition::Ge(a, b) => Filter::or(
                Filter::Gt(self.build_load_property(a)?, self.build_load_property(b)?),
                Filter::Eq(self.build_load_property(a)?, self.build_load_property(b)?),
            ),

            ast::Condition::IdEq(name, value) => match self.names.get(name).ok_or(Error::Todo)? {
                NamedEntity::Node(node) => Filter::NodeHasId {
                    node: *node,
                    id: self.build_load_property(value)?,
                },
                NamedEntity::Edge(edge) => Filter::EdgeHasId {
                    edge: *edge,
                    id: self.build_load_property(value)?,
                },
            },
        };
        Ok(filter)
    }

    fn build_filters_from_property_map(
        &mut self,
        edge_or_node: NamedEntity,
        property_map: &[(&'src str, ast::Expression<'src>)],
    ) -> Result<Vec<MatchStep>, Error> {
        property_map
            .iter()
            .map(|(key, value)| {
                Ok(MatchStep::Filter(match edge_or_node {
                    NamedEntity::Node(node) => Filter::Eq(
                        LoadProperty::PropertyOfNode {
                            node,
                            key: key.to_string(),
                        },
                        self.build_load_property(value)?,
                    ),
                    NamedEntity::Edge(edge) => Filter::Eq(
                        LoadProperty::PropertyOfEdge {
                            edge,
                            key: key.to_string(),
                        },
                        self.build_load_property(value)?,
                    ),
                }))
            })
            .collect()
    }

    fn build_match(&mut self, clause: &ast::MatchClause<'src>) -> Result<Vec<MatchStep>, Error> {
        let mut steps = vec![];

        // FIXME: this is an eyesore ...
        let mut prev_node_name = if let Some(name) = clause.start.annotation.name {
            if let Some(name) = self.get_node(name)? {
                name
            } else {
                let name = self.create_node(name)?;
                steps.push(MatchStep::LoadAnyNode { name });
                name
            }
        } else {
            let name = self.next_name();
            steps.push(MatchStep::LoadAnyNode { name });
            name
        };

        if let Some(label) = clause.start.annotation.label {
            steps.push(MatchStep::Filter(Filter::NodeHasLabel {
                node: prev_node_name,
                label: label.to_string(),
            }));
        }

        steps.append(&mut self.build_filters_from_property_map(
            NamedEntity::Node(prev_node_name),
            clause.start.properties.as_ref(),
        )?);

        for (edge, node) in &clause.edges {
            let edge_name = if let Some(name) = edge.annotation.name {
                if let Some(name) = self.get_edge(name)? {
                    match edge.direction {
                        ast::Direction::Left => steps.push(MatchStep::Filter(Filter::IsTarget {
                            node: prev_node_name,
                            edge: name,
                        })),
                        ast::Direction::Right => steps.push(MatchStep::Filter(Filter::IsOrigin {
                            node: prev_node_name,
                            edge: name,
                        })),
                        ast::Direction::Either => steps.push(MatchStep::Filter(Filter::or(
                            Filter::IsOrigin {
                                node: prev_node_name,
                                edge: name,
                            },
                            Filter::IsTarget {
                                node: prev_node_name,
                                edge: name,
                            },
                        ))),
                    }
                    name
                } else {
                    let name = self.create_edge(name)?;
                    match edge.direction {
                        ast::Direction::Left => steps.push(MatchStep::LoadTargetEdge {
                            name,
                            node: prev_node_name,
                        }),
                        ast::Direction::Right => steps.push(MatchStep::LoadOriginEdge {
                            name,
                            node: prev_node_name,
                        }),
                        ast::Direction::Either => steps.push(MatchStep::LoadEitherEdge {
                            name,
                            node: prev_node_name,
                        }),
                    }
                    name
                }
            } else {
                let name = self.next_name();
                match edge.direction {
                    ast::Direction::Left => steps.push(MatchStep::LoadTargetEdge {
                        name,
                        node: prev_node_name,
                    }),
                    ast::Direction::Right => steps.push(MatchStep::LoadOriginEdge {
                        name,
                        node: prev_node_name,
                    }),
                    ast::Direction::Either => steps.push(MatchStep::LoadEitherEdge {
                        name,
                        node: prev_node_name,
                    }),
                }
                name
            };

            if let Some(label) = edge.annotation.label {
                steps.push(MatchStep::Filter(Filter::EdgeHasLabel {
                    edge: edge_name,
                    label: label.to_string(),
                }));
            }

            steps.append(&mut self.build_filters_from_property_map(
                NamedEntity::Edge(edge_name),
                edge.properties.as_ref(),
            )?);

            prev_node_name = if let Some(name) = node.annotation.name {
                if let Some(name) = self.get_node(name)? {
                    match edge.direction {
                        ast::Direction::Left => steps.push(MatchStep::Filter(Filter::IsOrigin {
                            node: name,
                            edge: edge_name,
                        })),
                        ast::Direction::Right => steps.push(MatchStep::Filter(Filter::IsTarget {
                            node: name,
                            edge: edge_name,
                        })),
                        ast::Direction::Either => steps.push(MatchStep::Filter(Filter::or(
                            Filter::and(
                                Filter::IsOrigin {
                                    node: name,
                                    edge: edge_name,
                                },
                                Filter::IsTarget {
                                    node: prev_node_name,
                                    edge: edge_name,
                                },
                            ),
                            Filter::and(
                                Filter::IsTarget {
                                    node: name,
                                    edge: edge_name,
                                },
                                Filter::IsOrigin {
                                    node: prev_node_name,
                                    edge: edge_name,
                                },
                            ),
                        ))),
                    }
                    name
                } else {
                    let name = self.create_node(name)?;
                    match edge.direction {
                        ast::Direction::Left => steps.push(MatchStep::LoadOriginNode {
                            name,
                            edge: edge_name,
                        }),
                        ast::Direction::Right => steps.push(MatchStep::LoadTargetNode {
                            name,
                            edge: edge_name,
                        }),
                        ast::Direction::Either => steps.push(MatchStep::LoadOtherNode {
                            name,
                            node: prev_node_name,
                            edge: edge_name,
                        }),
                    }
                    name
                }
            } else {
                let name = self.next_name();
                match edge.direction {
                    ast::Direction::Left => steps.push(MatchStep::LoadOriginNode {
                        name,
                        edge: edge_name,
                    }),
                    ast::Direction::Right => steps.push(MatchStep::LoadTargetNode {
                        name,
                        edge: edge_name,
                    }),
                    ast::Direction::Either => steps.push(MatchStep::LoadOtherNode {
                        name,
                        node: prev_node_name,
                        edge: edge_name,
                    }),
                }
                name
            };

            if let Some(label) = node.annotation.label {
                steps.push(MatchStep::Filter(Filter::NodeHasLabel {
                    node: prev_node_name,
                    label: label.to_string(),
                }));
            }

            steps.append(&mut self.build_filters_from_property_map(
                NamedEntity::Node(prev_node_name),
                node.properties.as_ref(),
            )?);
        }

        Ok(steps)
    }

    fn build_create_update(
        &mut self,
        clause: &ast::CreateClause<'src>,
    ) -> Result<UpdateStep, Error> {
        match clause {
            ast::CreateClause::CreateNode {
                name,
                label,
                properties,
            } => Ok(UpdateStep::CreateNode {
                name: name
                    .map(|n| self.create_node(n))
                    .transpose()?
                    .unwrap_or_else(|| self.next_name()),
                label: label.to_string(),
                properties: properties
                    .iter()
                    .map(|(key, expr)| -> Result<_, Error> {
                        Ok((key.to_string(), self.build_load_property(expr)?))
                    })
                    .collect::<Result<_, Error>>()?,
            }),
            ast::CreateClause::CreateEdge {
                name,
                label,
                origin,
                target,
                properties,
            } => Ok(UpdateStep::CreateEdge {
                name: name
                    .map(|n| self.create_edge(n))
                    .transpose()?
                    .unwrap_or_else(|| self.next_name()),
                label: label.to_string(),
                origin: self.get_node(origin)?.ok_or(Error::Todo)?,
                target: self.get_node(target)?.ok_or(Error::Todo)?,
                properties: properties
                    .iter()
                    .map(|(key, expr)| -> Result<_, Error> {
                        Ok((key.to_string(), self.build_load_property(expr)?))
                    })
                    .collect::<Result<_, Error>>()?,
            }),
        }
    }

    fn build_set_update(&mut self, clause: &ast::SetClause<'src>) -> Result<UpdateStep, Error> {
        match self.names.get(clause.name) {
            Some(NamedEntity::Node(node)) => Ok(UpdateStep::SetNodeProperty {
                node: *node,
                key: clause.key.to_string(),
                value: self.build_load_property(&clause.value)?,
            }),
            Some(NamedEntity::Edge(edge)) => Ok(UpdateStep::SetEdgeProperty {
                edge: *edge,
                key: clause.key.to_string(),
                value: self.build_load_property(&clause.value)?,
            }),
            None => Err(Error::Todo),
        }
    }

    fn build_delete_update(&mut self, name: &str) -> Result<UpdateStep, Error> {
        match self.names.get(name) {
            Some(&NamedEntity::Node(node)) => Ok(UpdateStep::DeleteNode { node }),
            Some(&NamedEntity::Edge(edge)) => Ok(UpdateStep::DeleteEdge { edge }),
            None => Err(Error::Todo),
        }
    }
}

impl QueryPlan {
    pub fn new(query: &ast::Query) -> Result<QueryPlan, Error> {
        let mut env = BuildEnv::new();
        let mut steps = vec![];
        let mut updates = vec![];

        for clause in &query.match_clauses {
            steps.append(&mut env.build_match(clause)?);
        }

        for condition in &query.where_clauses {
            steps.push(MatchStep::Filter(env.build_filter(condition)?));
        }

        for clause in &query.create_clauses {
            updates.push(env.build_create_update(clause)?);
        }
        for clause in &query.set_clauses {
            updates.push(env.build_set_update(clause)?);
        }
        for name in &query.delete_clauses {
            updates.push(env.build_delete_update(name)?);
        }
        updates.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mut returns = Vec::with_capacity(query.return_clause.len());
        for expr in &query.return_clause {
            returns.push(env.build_load_property(expr)?);
        }

        Ok(QueryPlan {
            steps,
            updates,
            returns,
        })
    }
}
