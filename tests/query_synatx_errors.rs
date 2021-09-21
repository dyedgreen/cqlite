use gqlite::{Error, Graph};

macro_rules! assert_err {
    ($expr:expr, $err:pat) => {
        match $expr {
            $err => (),
            _ => assert!(false, "Unexpected {:?}", $expr.err().unwrap()),
        }
    };
}

#[test]
fn use_undefined_name() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("RETURN unknown.name"),
        Err(Error::UnknownIdentifier(_))
    );
    assert_err!(
        graph.prepare("CREATE (a) -[:TEST]-> (b)"),
        Err(Error::UnknownIdentifier(_))
    );
    assert_err!(
        graph.prepare("MATCH (a) SET b.age = 42"),
        Err(Error::UnknownIdentifier(_))
    );
    assert_err!(
        graph.prepare("MATCH (a) RETURN ID(a), LABEL(b)"),
        Err(Error::UnknownIdentifier(_))
    );
}

#[test]
fn use_existing_name() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) CREATE (a:NODE)"),
        Err(Error::IdentifierExists(_))
    );
    assert_err!(
        graph.prepare("MATCH (a) -[e]-> (b) CREATE (e:NODE)"),
        Err(Error::IdentifierExists(_))
    );
    assert_err!(
        graph.prepare("CREATE (a:NODE) CREATE (a:NODE2)"),
        Err(Error::IdentifierExists(_))
    );
}

#[test]
fn use_node_as_edge() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) -[a]-> (b)"),
        Err(Error::IdentifierIsNotEdge(_))
    );
}

#[test]
fn use_edge_as_node() {
    let graph = Graph::open_anon().unwrap();
    assert_err!(
        graph.prepare("MATCH (a) -[b]-> (b)"),
        Err(Error::IdentifierIsNotNode(_))
    );
}
