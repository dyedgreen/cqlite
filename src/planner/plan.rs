use crate::store::Property; // TODO: Should it directly use this?

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryPlan {
    pub steps: Vec<MatchStep>,
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

    IsTruthy(AccessValue),

    Eq(AccessValue, AccessValue),
    Lt(AccessValue, AccessValue),
    Gt(AccessValue, AccessValue),
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

/// TODO: Fix this naming nightmare ...
/// it could be called 'LoadValue'?
/// FIXME: The plan does not need to take
/// ownership here ... (and then these can
/// be Happy + Copy)
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AccessValue {
    Constant(Property),
    IdOfNode { node: usize },
    IdOfEdge { edge: usize },
    PropertyOfNode { node: usize, key: String },
    PropertyOfEdge { edge: usize, key: String },
}
