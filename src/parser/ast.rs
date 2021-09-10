#[derive(Debug, Clone, PartialEq)]
pub struct Query<'src> {
    pub match_clauses: Vec<MatchClause<'src>>,
    pub where_clauses: Vec<Condition<'src>>,
    pub return_clause: Vec<&'src str>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause<'src> {
    pub start: Node<'src>,
    pub edges: Vec<(Edge<'src>, Node<'src>)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Label<'src> {
    pub name: Option<&'src str>,
    pub kind: Option<&'src str>,
}

impl<'src> Label<'src> {
    pub fn new(name: &'src str, kind: &'src str) -> Self {
        Self {
            name: Some(name),
            kind: Some(kind),
        }
    }

    pub fn with_name(name: &'src str) -> Self {
        Self {
            name: Some(name),
            kind: None,
        }
    }

    pub fn with_kind(kind: &'src str) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Node<'src> {
    pub label: Label<'src>,
}

impl<'src> Node<'src> {
    pub fn with_label(label: Label<'src>) -> Self {
        Self { label }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Left,
    Right,
    Either,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Edge<'src> {
    pub direction: Direction,
    pub label: Label<'src>,
}

impl<'src> Edge<'src> {
    pub fn either(label: Label<'src>) -> Self {
        Self {
            direction: Direction::Either,
            label,
        }
    }

    pub fn left(label: Label<'src>) -> Self {
        Self {
            direction: Direction::Left,
            label,
        }
    }

    pub fn right(label: Label<'src>) -> Self {
        Self {
            direction: Direction::Right,
            label,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Literal<'src> {
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(&'src str),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Expression<'src> {
    Placeholder,
    Literal(Literal<'src>),
    Property { name: &'src str, key: &'src str },
    IdOf(&'src str),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition<'src> {
    And(Box<Condition<'src>>, Box<Condition<'src>>),
    Or(Box<Condition<'src>>, Box<Condition<'src>>),
    Not(Box<Condition<'src>>),

    Expression(Expression<'src>),

    Eq(Expression<'src>, Expression<'src>),
    Ne(Expression<'src>, Expression<'src>),

    Lt(Expression<'src>, Expression<'src>),
    Le(Expression<'src>, Expression<'src>),

    Gt(Expression<'src>, Expression<'src>),
    Ge(Expression<'src>, Expression<'src>),
}

impl<'src> Condition<'src> {
    pub fn and(a: Self, b: Self) -> Self {
        Self::And(Box::new(a), Box::new(b))
    }

    pub fn or(a: Self, b: Self) -> Self {
        Self::Or(Box::new(a), Box::new(b))
    }

    pub fn not(cond: Self) -> Self {
        Self::Not(Box::new(cond))
    }
}
