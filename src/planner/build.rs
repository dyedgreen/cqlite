use crate::Error;
use crate::{parser::ast, Property};
use std::collections::HashMap;

use super::plan::{Filter, LoadProperty, MatchStep, NamedEntity, QueryPlan};

pub(crate) struct BuildEnv<'a> {
    names: HashMap<&'a str, NamedEntity>,
    next_name: usize,
}

impl<'a> BuildEnv<'a> {
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

    fn create_node(&mut self, name: &'a str) -> Result<usize, Error> {
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

    fn create_edge(&mut self, name: &'a str) -> Result<usize, Error> {
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
        let access_value = match expr {
            ast::Expression::Placeholder => unimplemented!(),
            ast::Expression::Literal(literal) => LoadProperty::Constant(match literal {
                ast::Literal::Integer(i) => Property::Integer(*i),
                ast::Literal::Real(r) => Property::Real(*r),
                ast::Literal::Boolean(b) => Property::Boolean(*b),
                ast::Literal::Text(t) => Property::Text(t.to_string()),
                ast::Literal::Null => Property::Null,
            }),
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
        Ok(access_value)
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
}

impl QueryPlan {
    pub fn new(query: &ast::Query) -> Result<QueryPlan, Error> {
        if query.match_clauses.is_empty() && !query.where_clauses.is_empty() {
            return Err(Error::Todo);
        }
        if query.match_clauses.is_empty() && !query.return_clause.is_empty() {
            return Err(Error::Todo);
        }

        let mut env = BuildEnv::new();
        let mut steps = vec![];

        // FIXME: this is an eyesore ...
        for clause in &query.match_clauses {
            let mut prev_node_name = if let Some(name) = clause.start.annotation.name {
                if let Some(name) = env.get_node(name)? {
                    name
                } else {
                    let name = env.create_node(name)?;
                    steps.push(MatchStep::LoadAnyNode { name });
                    name
                }
            } else {
                let name = env.next_name();
                steps.push(MatchStep::LoadAnyNode { name });
                name
            };

            if let Some(label) = clause.start.annotation.label {
                steps.push(MatchStep::Filter(Filter::NodeHasLabel {
                    node: prev_node_name,
                    label: label.to_string(),
                }));
            }

            for (edge, node) in &clause.edges {
                let edge_name = if let Some(name) = edge.annotation.name {
                    if let Some(name) = env.get_edge(name)? {
                        match edge.direction {
                            ast::Direction::Left => {
                                steps.push(MatchStep::Filter(Filter::IsTarget {
                                    node: prev_node_name,
                                    edge: name,
                                }))
                            }
                            ast::Direction::Right => {
                                steps.push(MatchStep::Filter(Filter::IsOrigin {
                                    node: prev_node_name,
                                    edge: name,
                                }))
                            }
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
                        let name = env.create_edge(name)?;
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
                    let name = env.next_name();
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

                prev_node_name = if let Some(name) = node.annotation.name {
                    if let Some(name) = env.get_node(name)? {
                        match edge.direction {
                            ast::Direction::Left => {
                                steps.push(MatchStep::Filter(Filter::IsOrigin {
                                    node: name,
                                    edge: edge_name,
                                }))
                            }
                            ast::Direction::Right => {
                                steps.push(MatchStep::Filter(Filter::IsTarget {
                                    node: name,
                                    edge: edge_name,
                                }))
                            }
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
                        let name = env.create_node(name)?;
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
                    let name = env.next_name();
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
            }
        }

        for condition in &query.where_clauses {
            steps.push(MatchStep::Filter(env.build_filter(condition)?));
        }

        let mut returns = Vec::with_capacity(query.return_clause.len());
        for &name in &query.return_clause {
            returns.push(*env.names.get(name).ok_or(Error::Todo)?);
        }

        Ok(QueryPlan { steps, returns })
    }
}
