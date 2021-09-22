use super::Optimization;
use crate::planner::{Filter, MatchStep, QueryPlan};
use crate::Error;

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
