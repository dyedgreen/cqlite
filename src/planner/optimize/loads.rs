use super::Optimization;
use crate::planner::{Filter, LoadProperty, MatchStep, QueryPlan};
use crate::Error;
use std::collections::HashMap;

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
