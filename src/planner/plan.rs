#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryPlan {
    pub matches: Vec<MatchStep>,
    pub returns: Vec<NamedValue>,
}

/// A step in the logical query plan. The execution model
/// is to conceptually instantiate every combination of
/// possible nodes in order (think nested loops).
///
/// TODO: Describe this more clearly ...
#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Filter {
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),

    IsOrigin { node: usize, edge: usize },
    IsTarget { node: usize, edge: usize },
    NodeHasKind { node: usize, kind: String },
    EdgeHasKind { edge: usize, kind: String },
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
pub(crate) enum NamedValue {
    Node(usize),
    Edge(usize),
}
