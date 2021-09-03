#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Query<'a> {
    pub match_clauses: Vec<MatchClause<'a>>,
    pub return_clause: Vec<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub struct Label<'a> {
    pub name: Option<&'a str>,
    pub kind: Option<&'a str>,
}

impl<'a> Label<'a> {
    pub fn new(name: &'a str, kind: &'a str) -> Self {
        Self {
            name: Some(name),
            kind: Some(kind),
        }
    }

    pub fn with_name(name: &'a str) -> Self {
        Self {
            name: Some(name),
            kind: None,
        }
    }

    pub fn with_kind(kind: &'a str) -> Self {
        Self {
            name: None,
            kind: Some(kind),
        }
    }

    pub fn empty() -> Self {
        Self {
            name: None,
            kind: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub struct Node<'a> {
    pub label: Label<'a>,
}

impl<'a> Node<'a> {
    pub fn with_label(label: Label<'a>) -> Self {
        Self { label }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum Direction {
    Left,
    Right,
    Either,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub struct Edge<'a> {
    pub direction: Direction,
    pub label: Label<'a>,
}

impl<'a> Edge<'a> {
    pub fn either(label: Label<'a>) -> Self {
        Self {
            direction: Direction::Either,
            label,
        }
    }

    pub fn left(label: Label<'a>) -> Self {
        Self {
            direction: Direction::Left,
            label,
        }
    }

    pub fn right(label: Label<'a>) -> Self {
        Self {
            direction: Direction::Right,
            label,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct MatchClause<'a> {
    pub start: Node<'a>,
    pub edges: Vec<(Edge<'a>, Node<'a>)>,
}
