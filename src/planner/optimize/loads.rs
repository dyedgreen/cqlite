use super::Optimization;
use crate::planner::{Filter, LoadProperty, MatchStep, QueryPlan};
use crate::Error;
use std::collections::{HashMap, HashSet};
use std::iter::once;

/// Reorder to loads such that the initial `LoadAnyNode` has an
/// `WHERE ID(n) =` constraint if possible. This performs at most
/// one re-order for each apply.
pub(crate) struct ReorderIdConstrainedFirst;

impl Optimization for ReorderIdConstrainedFirst {
    fn apply(plan: &mut QueryPlan) -> Result<bool, Error> {
        // FIXME: Could this be implemented more cleanly?
        let id_constrained: HashSet<usize> = plan
            .steps
            .iter()
            .filter_map(|step| match step {
                MatchStep::Filter(
                    Filter::NodeHasId { node: name, .. } | Filter::EdgeHasId { edge: name, .. },
                ) => Some(*name),
                _ => None,
            })
            .collect();

        #[derive(Debug)]
        enum Path {
            IsExact,
            HasExactDep(usize),
        }
        let paths = plan
            .steps
            .iter()
            .rfold(HashMap::new(), |mut paths, step| match *step {
                MatchStep::LoadOriginNode { name, edge }
                | MatchStep::LoadTargetNode { name, edge }
                | MatchStep::LoadOtherNode { name, edge, .. } => {
                    if id_constrained.contains(&name) {
                        // this will over-write/ short-circuit longer
                        // paths to the shortest one
                        paths.insert(name, Path::IsExact);
                        paths.insert(edge, Path::HasExactDep(name));
                    } else if paths.contains_key(&name) {
                        paths.insert(edge, Path::HasExactDep(name));
                    }
                    paths
                }
                MatchStep::LoadOriginEdge { name, node }
                | MatchStep::LoadTargetEdge { name, node }
                | MatchStep::LoadEitherEdge { name, node } => {
                    if paths.contains_key(&name) {
                        paths.insert(node, Path::HasExactDep(name));
                    }
                    paths
                }
                _ => paths,
            });

        let start = plan.steps.iter().find_map(|step| match step {
            MatchStep::LoadAnyNode { name } => {
                if paths.contains_key(name) && !id_constrained.contains(name) {
                    Some(*name)
                } else {
                    None
                }
            }
            _ => None,
        });

        if let Some(start) = start {
            let mut current = Some(start);
            let path: Vec<usize> = plan
                .steps
                .iter()
                .enumerate()
                .filter_map(|(idx, step)| match *step {
                    MatchStep::LoadAnyNode { name }
                    | MatchStep::LoadOriginNode { name, .. }
                    | MatchStep::LoadTargetNode { name, .. }
                    | MatchStep::LoadOtherNode { name, .. }
                    | MatchStep::LoadOriginEdge { name, .. }
                    | MatchStep::LoadTargetEdge { name, .. }
                    | MatchStep::LoadEitherEdge { name, .. } => {
                        if current.map(|c| c == name).unwrap_or(false) {
                            current = match paths.get(&name) {
                                Some(Path::HasExactDep(next)) => Some(*next),
                                Some(Path::IsExact) => None,
                                None => unreachable!(),
                            };
                            Some(idx)
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .collect();
            debug_assert!(path.len() % 2 == 1);

            let reversed_path: Vec<_> = path
                .iter()
                .skip(1)
                .zip(path.iter().skip(2).map(|&idx| Some(idx)).chain(once(None)))
                .map(|(&idx, next_idx)| match plan.steps[idx] {
                    MatchStep::LoadTargetNode { name, edge } => MatchStep::LoadTargetEdge {
                        name: edge,
                        node: name,
                    },
                    MatchStep::LoadOriginNode { name, edge } => MatchStep::LoadOriginEdge {
                        name: edge,
                        node: name,
                    },
                    MatchStep::LoadOtherNode { name, edge, .. } => MatchStep::LoadEitherEdge {
                        name: edge,
                        node: name,
                    },
                    MatchStep::LoadTargetEdge { name, node } => MatchStep::LoadTargetNode {
                        name: node,
                        edge: name,
                    },
                    MatchStep::LoadOriginEdge { name, node } => MatchStep::LoadOriginNode {
                        name: node,
                        edge: name,
                    },
                    MatchStep::LoadEitherEdge { name, node } => {
                        let other = next_idx
                            .map(|i| match plan.steps[i] {
                                MatchStep::LoadOtherNode { name, .. } => name,
                                _ => unreachable!(),
                            })
                            .unwrap();
                        MatchStep::LoadOtherNode {
                            name: node,
                            edge: name,
                            node: other,
                        }
                    }
                    _ => unreachable!(),
                })
                .chain(once(match plan.steps[*path.last().unwrap()] {
                    MatchStep::LoadOriginNode { name, .. }
                    | MatchStep::LoadTargetNode { name, .. }
                    | MatchStep::LoadOtherNode { name, .. } => MatchStep::LoadAnyNode { name },
                    _ => unreachable!(),
                }))
                .collect();

            for (chunk, (&start, &end)) in
                path.iter().rev().skip(1).zip(path.iter().rev()).enumerate()
            {
                for idx in (start..end).rev() {
                    plan.steps[idx + 1 + chunk] = plan.steps[idx].clone();
                }
            }

            let start = path[0];
            for (idx, step) in reversed_path.into_iter().rev().enumerate() {
                plan.steps[idx + start] = step;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Transform pairs of `LoadAnyNode` and `NodeHasId` into
/// `LoadExactNode`.
pub(crate) struct LoadAnyToLoadExact;

impl Optimization for LoadAnyToLoadExact {
    fn apply(plan: &mut QueryPlan) -> Result<bool, Error> {
        let mut changed = false;
        let mut node_id_checks: HashMap<usize, LoadProperty> = plan
            .steps
            .iter()
            .filter_map(|step| match step {
                MatchStep::Filter(Filter::NodeHasId { node, id }) => Some((*node, id.clone())),
                _ => None,
            })
            .collect();
        plan.steps = plan
            .steps
            .drain(..)
            .filter_map(|step| match step {
                MatchStep::LoadAnyNode { name } => node_id_checks
                    .remove(&name)
                    .map(|id| {
                        changed = true;
                        MatchStep::LoadExactNode { name, id }
                    })
                    .or(Some(MatchStep::LoadAnyNode { name })),
                MatchStep::Filter(Filter::NodeHasId { node, id }) => {
                    if node_id_checks.contains_key(&node) {
                        Some(MatchStep::Filter(Filter::NodeHasId { node, id }))
                    } else {
                        None
                    }
                }
                step => Some(step),
            })
            .collect();
        Ok(changed)
    }
}

/// Transform pairs of `LoadAnyNode` and `NodeHasLabel` into
/// `LoadLabeledNode`.
pub(crate) struct LoadAnyToLoadLabeled;

impl Optimization for LoadAnyToLoadLabeled {
    fn apply(plan: &mut QueryPlan) -> Result<bool, Error> {
        let mut changed = false;
        let mut node_label_checks: HashMap<usize, &str> = plan
            .steps
            .iter()
            .filter_map(|step| match step {
                MatchStep::Filter(Filter::NodeHasLabel { node, label }) => Some((*node, *label)),
                _ => None,
            })
            .collect();
        plan.steps = plan
            .steps
            .drain(..)
            .filter_map(|step| match step {
                MatchStep::LoadAnyNode { name } => node_label_checks
                    .remove(&name)
                    .map(|label| {
                        changed = true;
                        MatchStep::LoadLabeledNode { name, label }
                    })
                    .or(Some(MatchStep::LoadAnyNode { name })),
                MatchStep::Filter(Filter::NodeHasLabel { node, label }) => {
                    if node_label_checks.contains_key(&node) {
                        Some(MatchStep::Filter(Filter::NodeHasLabel { node, label }))
                    } else {
                        None
                    }
                }
                step => Some(step),
            })
            .collect();
        Ok(changed)
    }
}
