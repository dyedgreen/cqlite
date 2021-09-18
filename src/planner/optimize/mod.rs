use super::QueryPlan;
use crate::Error;

impl<'src> QueryPlan<'src> {
    pub fn optimize(self) -> Result<Self, Error> {
        // TODO
        Ok(self)
    }
}
