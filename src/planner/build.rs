use crate::parser::ast;
use crate::Error;
use std::collections::HashMap;

use super::plan::{MatchStep, NamedValue, QueryPlan};

pub(crate) struct BuildEnv<'a> {
    names: HashMap<&'a str, NamedValue>,
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
            Some(NamedValue::Node(name)) => Ok(Some(*name)),
            Some(NamedValue::Edge(_)) => Err(Error::Todo),
            None => Ok(None),
        }
    }

    fn get_edge(&self, name: &str) -> Result<Option<usize>, Error> {
        match self.names.get(&name) {
            Some(NamedValue::Node(_)) => Err(Error::Todo),
            Some(NamedValue::Edge(name)) => Ok(Some(*name)),
            None => Ok(None),
        }
    }

    fn create_node(&mut self, name: &'a str) -> Result<usize, Error> {
        match self.names.get(&name) {
            Some(NamedValue::Node(name)) => Ok(*name),
            Some(NamedValue::Edge(_)) => Err(Error::Todo),
            None => {
                let next_name = self.next_name();
                self.names.insert(name, NamedValue::Node(next_name));
                Ok(next_name)
            }
        }
    }

    fn create_edge(&mut self, name: &'a str) -> Result<usize, Error> {
        match self.names.get(&name) {
            Some(NamedValue::Node(_)) => Err(Error::Todo),
            Some(NamedValue::Edge(name)) => Ok(*name),
            None => {
                let next_name = self.next_name();
                self.names.insert(name, NamedValue::Edge(next_name));
                Ok(next_name)
            }
        }
    }
}

impl QueryPlan {
    pub fn new(query: &ast::Query) -> Result<QueryPlan, Error> {
        let mut env = BuildEnv::new();
        let mut matches = vec![];

        for clause in &query.match_clauses {
            assert!(clause.start.label.kind.is_none()); // TODO

            let mut prev_node_name = if let Some(name) = clause.start.label.name {
                if let Some(name) = env.get_node(name)? {
                    name
                } else {
                    let name = env.create_node(name)?;
                    matches.push(MatchStep::LoadAnyNode { name });
                    name
                }
            } else {
                let name = env.next_name();
                matches.push(MatchStep::LoadAnyNode { name });
                name
            };

            for (edge, node) in &clause.edges {
                assert!(edge.label.kind.is_none()); // TODO

                let edge_name = if let Some(name) = edge.label.name {
                    if let Some(name) = env.get_edge(name)? {
                        match edge.direction {
                            ast::Direction::Left => matches.push(MatchStep::FilterIsTarget {
                                node: prev_node_name,
                                edge: name,
                            }),
                            ast::Direction::Right => matches.push(MatchStep::FilterIsOrigin {
                                node: prev_node_name,
                                edge: name,
                            }),
                            ast::Direction::Either => unimplemented!(),
                        }
                        name
                    } else {
                        let name = env.create_edge(name)?;
                        match edge.direction {
                            ast::Direction::Left => matches.push(MatchStep::LoadTargetEdge {
                                name,
                                node: prev_node_name,
                            }),
                            ast::Direction::Right => matches.push(MatchStep::LoadOriginEdge {
                                name,
                                node: prev_node_name,
                            }),
                            ast::Direction::Either => unimplemented!(),
                        }
                        name
                    }
                } else {
                    let name = env.next_name();
                    match edge.direction {
                        ast::Direction::Left => matches.push(MatchStep::LoadTargetEdge {
                            name,
                            node: prev_node_name,
                        }),
                        ast::Direction::Right => matches.push(MatchStep::LoadOriginEdge {
                            name,
                            node: prev_node_name,
                        }),
                        ast::Direction::Either => unimplemented!(),
                    }
                    name
                };

                prev_node_name = if let Some(name) = node.label.name {
                    if let Some(name) = env.get_node(name)? {
                        match edge.direction {
                            ast::Direction::Left => matches.push(MatchStep::FilterIsOrigin {
                                node: name,
                                edge: edge_name,
                            }),
                            ast::Direction::Right => matches.push(MatchStep::FilterIsTarget {
                                node: name,
                                edge: edge_name,
                            }),
                            ast::Direction::Either => unimplemented!(),
                        }
                        name
                    } else {
                        let name = env.create_node(name)?;
                        match edge.direction {
                            ast::Direction::Left => matches.push(MatchStep::LoadOriginNode {
                                name,
                                edge: edge_name,
                            }),
                            ast::Direction::Right => matches.push(MatchStep::LoadTargetNode {
                                name,
                                edge: edge_name,
                            }),
                            ast::Direction::Either => unimplemented!(),
                        }
                        name
                    }
                } else {
                    let name = env.next_name();
                    match edge.direction {
                        ast::Direction::Left => matches.push(MatchStep::LoadOriginNode {
                            name,
                            edge: edge_name,
                        }),
                        ast::Direction::Right => matches.push(MatchStep::LoadTargetNode {
                            name,
                            edge: edge_name,
                        }),
                        ast::Direction::Either => unimplemented!(),
                    }
                    name
                };
            }
        }

        let mut returns = Vec::with_capacity(query.return_clause.len());
        for &name in &query.return_clause {
            returns.push(*env.names.get(name).ok_or(Error::Todo)?);
        }

        Ok(QueryPlan { matches, returns })
    }
}
