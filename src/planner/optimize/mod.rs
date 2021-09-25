use super::QueryPlan;
use crate::Error;

mod loads;
mod normalize;
#[cfg(test)]
mod tests;

const MAX_FIX_RUNS: usize = 1000;

pub(crate) trait Optimization {
    /// Apply the given optimization to the query plan. Returns
    /// if the plan was changed by the optimization pass.
    fn apply(plan: &mut QueryPlan) -> Result<bool, Error>;

    /// Run the optimization to a fixed point. Returns if the
    /// last application of the pass changed the query.
    fn fix(plan: &mut QueryPlan) -> Result<bool, Error> {
        for _ in 0..MAX_FIX_RUNS {
            if !Self::apply(plan)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl<'src> QueryPlan<'src> {
    pub fn optimize(mut self) -> Result<Self, Error> {
        normalize::SplitTopLevelAnd::fix(&mut self)?;
        normalize::MergeDuplicateUpdates::apply(&mut self)?;
        loads::ReorderIdConstrainedFirst::fix(&mut self)?;
        loads::LoadAnyToLoadExact::apply(&mut self)?;
        Ok(self)
    }
}
