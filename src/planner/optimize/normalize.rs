use super::Optimization;
use crate::planner::{Filter, MatchStep, QueryPlan, UpdateStep};
use crate::Error;
use std::collections::HashSet;

/// Split filters with a top level `AND` clause into multiple
/// filters.
pub(crate) struct SplitTopLevelAnd;

impl Optimization for SplitTopLevelAnd {
    fn apply(plan: &mut QueryPlan) -> Result<bool, Error> {
        let mut changed = false;
        plan.steps = plan
            .steps
            .drain(..)
            .flat_map(|step| match step {
                MatchStep::Filter(filter) => match filter {
                    Filter::And(lhs, rhs) => {
                        changed = true;
                        vec![MatchStep::Filter(*lhs), MatchStep::Filter(*rhs)]
                    }
                    filter => vec![MatchStep::Filter(filter)],
                },
                step => vec![step],
            })
            .collect();
        Ok(changed)
    }
}

/// Combine sets for the same node/ edge and property into a single
/// set. Combine deletes for the same node/ edge into a single delete.
pub(crate) struct MergeDuplicateUpdates;

impl Optimization for MergeDuplicateUpdates {
    fn apply(plan: &mut QueryPlan) -> Result<bool, Error> {
        let mut changed = false;
        let mut seen_deletes = HashSet::new();
        plan.updates = plan
            .updates
            .drain(..)
            .filter(|update| match update {
                UpdateStep::DeleteNode { node: name } | UpdateStep::DeleteEdge { edge: name } => {
                    if seen_deletes.contains(name) {
                        changed = true;
                        false
                    } else {
                        seen_deletes.insert(*name);
                        true
                    }
                }
                _ => true,
            })
            .collect();
        let mut seen_sets = HashSet::new();
        plan.updates = plan
            .updates
            .drain(..)
            .rev()
            .filter(|update| match update {
                UpdateStep::SetNodeProperty {
                    node: name, key, ..
                }
                | UpdateStep::SetEdgeProperty {
                    edge: name, key, ..
                } => {
                    let pair = (*name, *key);
                    if seen_sets.contains(&pair) {
                        changed = true;
                        false
                    } else {
                        seen_sets.insert(pair);
                        true
                    }
                }
                _ => true,
            })
            .collect();
        plan.updates.reverse();
        Ok(changed)
    }
}
