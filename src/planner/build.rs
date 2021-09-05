use crate::parser::ast;
use crate::Error;
use std::collections::HashMap;
use std::rc::Rc;

use super::plan::{LoadEdge, LoadNode, MatchEdge, MatchNode};
use super::QueryPlan;

pub(crate) struct BuildEnv<'a> {
    names: HashMap<&'a str, (usize, NameType)>,
    next_name: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameType {
    Node,
    Edge,
}

impl<'a> BuildEnv<'a> {
    fn get_name(&mut self, name: Option<&'a str>, name_type: NameType) -> Result<usize, Error> {
        if let Some(name) = name {
            match self.names.get(&name) {
                Some((name, typ)) if *typ == name_type => Ok(*name),
                Some(_) => Err(Error::Todo),
                None => {
                    self.names.insert(name, (self.next_name, name_type));
                    self.next_name += 1;
                    Ok(self.next_name - 1)
                }
            }
        } else {
            self.next_name += 1;
            Ok(self.next_name - 1)
        }
    }

    fn get_node_name(&mut self, name: Option<&'a str>) -> Result<usize, Error> {
        self.get_name(name, NameType::Node)
    }

    fn get_edge_name(&mut self, name: Option<&'a str>) -> Result<usize, Error> {
        self.get_name(name, NameType::Edge)
    }
}

fn build_match_clause<'a>(
    clause: &ast::MatchClause<'a>,
    env: &mut BuildEnv<'a>,
) -> Result<MatchNode, Error> {
    fn build_connection<'a>(
        prev: usize,
        env: &mut BuildEnv<'a>,
        edges: &[(ast::Edge<'a>, ast::Node<'a>)],
    ) -> Result<Option<Rc<MatchEdge>>, Error> {
        Ok(edges
            .get(0)
            .map(|(edge, node)| -> Result<Rc<MatchEdge>, Error> {
                Ok(match edge.direction {
                    ast::Direction::Either => unimplemented!(),
                    ast::Direction::Left => {
                        let edge_name = env.get_edge_name(edge.label.name)?;
                        let node_name = env.get_node_name(node.label.name)?;
                        // START: TODO
                        assert!(node.label.kind.is_none());
                        assert!(edge.label.kind.is_none());
                        // END: TODO
                        Rc::new(MatchEdge {
                            name: edge_name,
                            load: LoadEdge::Origin(prev),
                            next: MatchNode {
                                name: node_name,
                                load: LoadNode::Target(edge_name),
                                next: build_connection(node_name, env, &edges[1..])?,
                            },
                        })
                    }
                    ast::Direction::Right => unimplemented!(),
                })
            })
            .transpose()?)
    }

    assert!(clause.start.label.kind.is_none()); // TODO
    let name = env.get_node_name(clause.start.label.name)?;
    Ok(MatchNode {
        name: name,
        load: LoadNode::Any,
        next: build_connection(name, env, clause.edges.as_slice())?,
    })
}

pub(crate) fn build_plan(query: &ast::Query) -> Result<QueryPlan, Error> {
    assert_eq!(1, query.match_clauses.len()); // TODO

    let mut env = BuildEnv {
        names: HashMap::new(),
        next_name: 0,
    };

    let mut match_clauses = Vec::with_capacity(query.match_clauses.len());
    for clause in &query.match_clauses {
        match_clauses.push(build_match_clause(clause, &mut env)?);
    }
    let return_clause = Vec::new();

    Ok(QueryPlan {
        match_clauses,
        return_clause,
    })
}
