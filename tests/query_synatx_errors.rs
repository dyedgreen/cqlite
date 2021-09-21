use gqlite::{Error, Graph};

macro_rules! assert_err {
    ($expr:expr, $err:pat) => {
        match $expr {
            Err($err) => (),
            _ => assert!(false, "Unexpected {}", $expr.err().unwrap()),
        }
    };
}

#[test]
fn use_undefined_name() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("RETURN unknown.name"),
        Error::UnknownIdentifier(_)
    );
    assert_err!(
        graph.prepare("CREATE (a) -[:TEST]-> (b)"),
        Error::UnknownIdentifier(_)
    );
    assert_err!(
        graph.prepare("MATCH (a) SET b.age = 42"),
        Error::UnknownIdentifier(_)
    );
    assert_err!(
        graph.prepare("MATCH (a) RETURN ID(a), LABEL(b)"),
        Error::UnknownIdentifier(_)
    );
}

#[test]
fn use_existing_name() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) CREATE (a:NODE)"),
        Error::IdentifierExists(_)
    );
    assert_err!(
        graph.prepare("MATCH (a) -[e]-> (b) CREATE (e:NODE)"),
        Error::IdentifierExists(_)
    );
    assert_err!(
        graph.prepare("CREATE (a:NODE) CREATE (a:NODE2)"),
        Error::IdentifierExists(_)
    );
}

#[test]
fn use_node_as_edge() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) -[a]-> (b)"),
        Error::IdentifierIsNotEdge(_)
    );
}

#[test]
fn use_edge_as_node() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) -[b]-> (b)"),
        Error::IdentifierIsNotNode(_)
    );
}

#[test]
fn create_without_label() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(graph.prepare("CREATE (a)"), Error::Syntax { .. });
    assert_err!(
        graph.prepare("MATCH (a) CREATE (a) -> (b)"),
        Error::Syntax { .. }
    );
    assert_err!(
        graph.prepare("MATCH (a) CREATE (a) <-[{ test: 42 }]- (b)"),
        Error::Syntax { .. }
    );
}

#[test]
fn create_undirected_edge() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) CREATE (a) -[:LABEL]- (b)"),
        Error::Syntax { .. }
    );
}
