#[derive(Debug, Clone, PartialEq)]
pub struct Query<'src> {
    pub match_clauses: Vec<MatchClause<'src>>,
    pub where_clauses: Vec<Condition<'src>>,
    pub set_clauses: Vec<SetClause<'src>>,
    pub delete_clauses: Vec<&'src str>,
    pub return_clause: Vec<Expression<'src>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause<'src> {
    pub start: Node<'src>,
    pub edges: Vec<(Edge<'src>, Node<'src>)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SetClause<'src> {
    pub name: &'src str,
    pub key: &'src str,
    pub value: Expression<'src>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Annotation<'src> {
    pub name: Option<&'src str>,
    pub label: Option<&'src str>,
}

impl<'src> Annotation<'src> {
    pub fn new(name: &'src str, kind: &'src str) -> Self {
        Self {
            name: Some(name),
            label: Some(kind),
        }
    }

    pub fn with_name(name: &'src str) -> Self {
        Self {
            name: Some(name),
            label: None,
        }
    }

    pub fn with_label(label: &'src str) -> Self {
        Self {
            name: None,
            label: Some(label),
        }
    }

    pub fn empty() -> Self {
        Self {
            name: None,
            label: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node<'src> {
    pub annotation: Annotation<'src>,
    pub properties: Vec<(&'src str, Expression<'src>)>,
}

impl<'src> Node<'src> {
    pub fn new(
        annotation: Annotation<'src>,
        properties: Vec<(&'src str, Expression<'src>)>,
    ) -> Self {
        Self {
            annotation,
            properties,
        }
    }

    pub fn with_annotation(annotation: Annotation<'src>) -> Self {
        Self {
            annotation,
            properties: Vec::new(),
        }
    }

    pub fn with_properties(properties: Vec<(&'src str, Expression<'src>)>) -> Self {
        Self {
            annotation: Annotation::empty(),
            properties,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Left,
    Right,
    Either,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Edge<'src> {
    pub direction: Direction,
    pub annotation: Annotation<'src>,
    pub properties: Vec<(&'src str, Expression<'src>)>,
}

impl<'src> Edge<'src> {
    pub fn either(
        annotation: Annotation<'src>,
        properties: Vec<(&'src str, Expression<'src>)>,
    ) -> Self {
        Self {
            direction: Direction::Either,
            annotation,
            properties,
        }
    }

    pub fn left(
        annotation: Annotation<'src>,
        properties: Vec<(&'src str, Expression<'src>)>,
    ) -> Self {
        Self {
            direction: Direction::Left,
            annotation,
            properties,
        }
    }

    pub fn right(
        annotation: Annotation<'src>,
        properties: Vec<(&'src str, Expression<'src>)>,
    ) -> Self {
        Self {
            direction: Direction::Right,
            annotation,
            properties,
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
    Literal(Literal<'src>),
    IdOf { name: &'src str },
    Property { name: &'src str, key: &'src str },
    Parameter(&'src str),
}

impl<'src> Expression<'src> {
    pub fn id_of(name: &'src str) -> Self {
        Self::IdOf { name }
    }

    pub fn property(name: &'src str, key: &'src str) -> Self {
        Self::Property { name, key }
    }
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

    IdEq(&'src str, Expression<'src>),
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
