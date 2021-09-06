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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchStep {
    LoadAnyNode { name: usize },
    LoadOriginNode { name: usize, edge: usize },
    LoadTargetNode { name: usize, edge: usize },

    LoadOriginEdge { name: usize, node: usize },
    LoadTargetEdge { name: usize, node: usize },

    FilterIsOrigin { node: usize, edge: usize },
    FilterIsTarget { node: usize, edge: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum NamedValue {
    Node(usize),
    Edge(usize),
}
