use super::*;

#[test]
fn id_seq_works() {
    let store = Store::open_anon().unwrap();

    let txn = store.mut_txn().unwrap();
    assert_eq!(0, txn.id_seq());
    assert_eq!(1, txn.id_seq());
    assert_eq!(2, txn.id_seq());
    txn.commit().unwrap();

    let txn = store.mut_txn().unwrap();
    assert_eq!(3, txn.id_seq());
    assert_eq!(4, txn.id_seq());
    assert_eq!(5, txn.id_seq());
    txn.commit().unwrap();
}

#[test]
fn create_nodes_and_edges() {
    let store = Store::open("test.graph").unwrap();
    let mut txn = store.mut_txn().unwrap();
    let node1 = txn
        .unchecked_create_node(Node {
            id: txn.id_seq(),
            label: "PERSON".to_string(),
            properties: Default::default(),
        })
        .unwrap();
    let node2 = txn
        .unchecked_create_node(Node {
            id: txn.id_seq(),
            label: "PERSON".to_string(),
            properties: Default::default(),
        })
        .unwrap();
    let edge = txn
        .unchecked_create_edge(Edge {
            id: txn.id_seq(),
            label: "KNOWS".to_string(),
            origin: node1.id(),
            target: node2.id(),
            properties: Default::default(),
        })
        .unwrap();
    txn.commit().unwrap();

    let txn = store.txn().unwrap();
    let node1 = txn.load_node(node1.id()).unwrap().unwrap();
    let node2 = txn.load_node(node2.id()).unwrap().unwrap();
    let edge = txn.load_edge(edge.id()).unwrap().unwrap();

    assert_eq!(node1.label(), "PERSON");
    assert_eq!(node2.label(), "PERSON");
    assert_eq!(edge.label(), "KNOWS");
}

#[test]
fn update_nodes_and_edges() {
    let store = Store::open_anon().unwrap();
    let mut txn = store.mut_txn().unwrap();
    let node = txn
        .unchecked_create_node(Node {
            id: txn.id_seq(),
            label: "PERSON".to_string(),
            properties: Default::default(),
        })
        .unwrap();
    let edge = txn
        .unchecked_create_edge(Edge {
            id: txn.id_seq(),
            label: "KNOWS".to_string(),
            origin: node.id(),
            target: node.id(),
            properties: Default::default(),
        })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = store.mut_txn().unwrap();
    txn.update_node(node.id(), "test", PropOwned::Integer(42))
        .unwrap();
    txn.update_edge(edge.id(), "test", PropOwned::Real(42.0))
        .unwrap();
    txn.commit().unwrap();

    let txn = store.txn().unwrap();
    let node = txn.load_node(node.id()).unwrap().unwrap();
    let edge = txn.load_edge(edge.id()).unwrap().unwrap();

    assert_eq!(node.property("test"), &PropOwned::Integer(42));
    assert_eq!(edge.property("test"), &PropOwned::Real(42.0));
}

#[test]
fn delete_nodes_and_edges() {
    let store = Store::open_anon().unwrap();
    let mut txn = store.mut_txn().unwrap();
    let node = txn
        .unchecked_create_node(Node {
            id: txn.id_seq(),
            label: "PERSON".to_string(),
            properties: Default::default(),
        })
        .unwrap();
    let edge = txn
        .unchecked_create_edge(Edge {
            id: txn.id_seq(),
            label: "KNOWS".to_string(),
            origin: node.id(),
            target: node.id(),
            properties: Default::default(),
        })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = store.mut_txn().unwrap();
    assert!(txn.load_node(node.id()).unwrap().is_some());
    assert!(txn.load_edge(edge.id()).unwrap().is_some());

    txn.delete_edge(edge.id()).unwrap();
    txn.delete_node(node.id()).unwrap();
    txn.commit().unwrap();

    let txn = store.txn().unwrap();
    assert!(txn.load_node(node.id()).unwrap().is_none());
    assert!(txn.load_edge(edge.id()).unwrap().is_none());
}
