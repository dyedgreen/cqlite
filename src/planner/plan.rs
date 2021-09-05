use std::rc::Rc;

// TODO: Might be better to be slightly lower level (?)
// like : LoadNode, LoadEdge, CheckNode, ... (?)
// I think so ... (this allows to combine multiple matches later one ...)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadNode {
    Any,           // iterate all nodes / edges
    Named(usize),  // refer to already loaded node/ edge
    Origin(usize), // origin node of edge / iter edges originating from node
    Target(usize), // target node of edge / iter edges targeting node
                   // TODO: indexing on kind, will need special case (?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadEdge {
    Named(usize),
    Origin(usize),
    Target(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchNode {
    pub name: usize,
    pub load: LoadNode,
    pub next: Option<Rc<MatchEdge>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchEdge {
    pub name: usize,
    pub load: LoadEdge,
    pub next: MatchNode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReturnValue {
    Node(usize),
    Edge(usize),
}
