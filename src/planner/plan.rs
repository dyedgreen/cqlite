use crate::store::Property;
use std::cmp::{Ordering, PartialOrd};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryPlan<'src> {
    pub steps: Vec<MatchStep<'src>>,
    pub updates: Vec<UpdateStep<'src>>,
    pub returns: Vec<LoadProperty<'src>>,
}

/// A step in the logical query plan. The execution model
/// is to conceptually instantiate every combination of
/// possible nodes in order (think nested loops).
///
/// TODO: Describe this more clearly ...
#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MatchStep<'src> {
    LoadAnyNode { name: usize },
    LoadExactNode { name: usize, id: LoadProperty<'src> },
    LoadOriginNode { name: usize, edge: usize },
    LoadTargetNode { name: usize, edge: usize },
    LoadOtherNode { name: usize, node: usize, edge: usize },

    LoadOriginEdge { name: usize, node: usize },
    LoadTargetEdge { name: usize, node: usize },
    LoadEitherEdge { name: usize, node: usize },

    Filter(Filter<'src>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Filter<'src> {
    And(Box<Filter<'src>>, Box<Filter<'src>>),
    Or(Box<Filter<'src>>, Box<Filter<'src>>),
    Not(Box<Filter<'src>>),

    IsOrigin { node: usize, edge: usize },
    IsTarget { node: usize, edge: usize },

    NodeHasLabel { node: usize, label: &'src str },
    EdgeHasLabel { edge: usize, label: &'src str },

    NodeHasId { node: usize, id: LoadProperty<'src> },
    EdgeHasId { edge: usize, id: LoadProperty<'src> },

    IsTruthy(LoadProperty<'src>),

    Eq(LoadProperty<'src>, LoadProperty<'src>),
    Lt(LoadProperty<'src>, LoadProperty<'src>),
    Gt(LoadProperty<'src>, LoadProperty<'src>),
}

impl<'src> Filter<'src> {
    pub fn and(a: Self, b: Self) -> Self {
        Self::And(Box::new(a), Box::new(b))
    }

    pub fn or(a: Self, b: Self) -> Self {
        Self::Or(Box::new(a), Box::new(b))
    }

    pub fn not(filter: Self) -> Self {
        Self::Not(Box::new(filter))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LoadProperty<'src> {
    Constant(Property),
    IdOfNode { node: usize },
    IdOfEdge { edge: usize },
    LabelOfNode { node: usize },
    LabelOfEdge { edge: usize },
    PropertyOfNode { node: usize, key: &'src str },
    PropertyOfEdge { edge: usize, key: &'src str },
    Parameter { name: &'src str },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum UpdateStep<'src> {
    CreateNode {
        name: usize,
        label: &'src str,
        properties: Vec<(&'src str, LoadProperty<'src>)>,
    },
    CreateEdge {
        name: usize,
        label: &'src str,
        origin: usize,
        target: usize,
        properties: Vec<(&'src str, LoadProperty<'src>)>,
    },
    SetNodeProperty {
        node: usize,
        key: &'src str,
        value: LoadProperty<'src>,
    },
    SetEdgeProperty {
        edge: usize,
        key: &'src str,
        value: LoadProperty<'src>,
    },
    DeleteNode {
        node: usize,
    },
    DeleteEdge {
        edge: usize,
    },
}

impl<'src> PartialOrd for UpdateStep<'src> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use UpdateStep::*;
        match (self, other) {
            (CreateNode { .. }, CreateNode { .. }) => Some(Ordering::Equal),
            (CreateEdge { .. }, CreateEdge { .. }) => Some(Ordering::Equal),
            (
                SetNodeProperty { .. } | SetEdgeProperty { .. },
                SetNodeProperty { .. } | SetEdgeProperty { .. },
            ) => Some(Ordering::Equal),
            (DeleteNode { .. }, DeleteNode { .. }) => Some(Ordering::Equal),
            (DeleteEdge { .. }, DeleteEdge { .. }) => Some(Ordering::Equal),

            (CreateNode { .. }, CreateEdge { .. }) => Some(Ordering::Less),
            (CreateEdge { .. }, CreateNode { .. }) => Some(Ordering::Greater),

            (
                CreateNode { .. } | CreateEdge { .. },
                SetNodeProperty { .. }
                | SetEdgeProperty { .. }
                | DeleteNode { .. }
                | DeleteEdge { .. },
            ) => Some(Ordering::Less),
            (
                SetNodeProperty { .. }
                | SetEdgeProperty { .. }
                | DeleteNode { .. }
                | DeleteEdge { .. },
                CreateNode { .. } | CreateEdge { .. },
            ) => Some(Ordering::Greater),

            (
                SetNodeProperty { .. } | SetEdgeProperty { .. },
                DeleteNode { .. } | DeleteEdge { .. },
            ) => Some(Ordering::Less),
            (
                DeleteNode { .. } | DeleteEdge { .. },
                SetNodeProperty { .. } | SetEdgeProperty { .. },
            ) => Some(Ordering::Greater),

            (DeleteEdge { .. }, DeleteNode { .. }) => Some(Ordering::Less),
            (DeleteNode { .. }, DeleteEdge { .. }) => Some(Ordering::Greater),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_step_order() {
        let mut steps = vec![
            UpdateStep::DeleteNode { node: 1 },
            UpdateStep::SetEdgeProperty {
                edge: 0,
                key: "test",
                value: LoadProperty::Parameter { name: "test" },
            },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "test",
                value: LoadProperty::Parameter { name: "test" },
            },
            UpdateStep::DeleteEdge { edge: 2 },
        ];
        steps.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let steps_ord = vec![
            UpdateStep::SetEdgeProperty {
                edge: 0,
                key: "test",
                value: LoadProperty::Parameter { name: "test" },
            },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "test",
                value: LoadProperty::Parameter { name: "test" },
            },
            UpdateStep::DeleteEdge { edge: 2 },
            UpdateStep::DeleteNode { node: 1 },
        ];

        assert_eq!(steps, steps_ord);
    }
}
