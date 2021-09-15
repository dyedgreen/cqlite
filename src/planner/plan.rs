use crate::store::Property;
use std::cmp::{Ordering, PartialOrd};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryPlan {
    pub steps: Vec<MatchStep>,
    pub updates: Vec<UpdateStep>,
    pub returns: Vec<NamedEntity>,
}

/// A step in the logical query plan. The execution model
/// is to conceptually instantiate every combination of
/// possible nodes in order (think nested loops).
///
/// TODO: Describe this more clearly ...
#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MatchStep {
    LoadAnyNode { name: usize },
    LoadOriginNode { name: usize, edge: usize },
    LoadTargetNode { name: usize, edge: usize },
    LoadOtherNode { name: usize, node: usize, edge: usize },

    LoadOriginEdge { name: usize, node: usize },
    LoadTargetEdge { name: usize, node: usize },
    LoadEitherEdge { name: usize, node: usize },

    Filter(Filter),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Filter {
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),

    IsOrigin { node: usize, edge: usize },
    IsTarget { node: usize, edge: usize },

    NodeHasLabel { node: usize, label: String },
    EdgeHasLabel { edge: usize, label: String },

    NodeHasId { node: usize, id: LoadProperty },
    EdgeHasId { edge: usize, id: LoadProperty },

    IsTruthy(LoadProperty),

    Eq(LoadProperty, LoadProperty),
    Lt(LoadProperty, LoadProperty),
    Gt(LoadProperty, LoadProperty),
}

impl Filter {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum NamedEntity {
    Node(usize),
    Edge(usize),
}

/// FIXME: The plan does not need to take
/// ownership here ... (and then these can
/// be Happy + Copy)
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LoadProperty {
    Constant(Property),
    PropertyOfNode { node: usize, key: String },
    PropertyOfEdge { edge: usize, key: String },
    Parameter { name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum UpdateStep {
    SetNodeProperty {
        node: usize,
        key: String,
        value: LoadProperty,
    },
    SetEdgeProperty {
        edge: usize,
        key: String,
        value: LoadProperty,
    },
    DeleteNode {
        node: usize,
    },
    DeleteEdge {
        edge: usize,
    },
}

impl PartialOrd for UpdateStep {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use UpdateStep::*;
        match (self, other) {
            (
                SetNodeProperty { .. } | SetEdgeProperty { .. },
                SetNodeProperty { .. } | SetEdgeProperty { .. },
            ) => Some(Ordering::Equal),
            (DeleteNode { .. }, DeleteNode { .. }) => Some(Ordering::Equal),
            (DeleteEdge { .. }, DeleteEdge { .. }) => Some(Ordering::Equal),
            (
                SetNodeProperty { .. } | SetEdgeProperty { .. },
                DeleteNode { .. } | DeleteEdge { .. },
            ) => Some(Ordering::Less),
            (
                DeleteNode { .. } | DeleteEdge { .. },
                SetNodeProperty { .. } | SetEdgeProperty { .. },
            ) => Some(Ordering::Greater),
            (DeleteNode { .. }, DeleteEdge { .. }) => Some(Ordering::Greater),
            (DeleteEdge { .. }, DeleteNode { .. }) => Some(Ordering::Less),
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
                key: "test".into(),
                value: LoadProperty::Parameter {
                    name: "test".into(),
                },
            },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "test".into(),
                value: LoadProperty::Parameter {
                    name: "test".into(),
                },
            },
            UpdateStep::DeleteEdge { edge: 2 },
        ];
        steps.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let steps_ord = vec![
            UpdateStep::SetEdgeProperty {
                edge: 0,
                key: "test".into(),
                value: LoadProperty::Parameter {
                    name: "test".into(),
                },
            },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "test".into(),
                value: LoadProperty::Parameter {
                    name: "test".into(),
                },
            },
            UpdateStep::DeleteEdge { edge: 2 },
            UpdateStep::DeleteNode { node: 1 },
        ];

        assert_eq!(steps, steps_ord);
    }
}
