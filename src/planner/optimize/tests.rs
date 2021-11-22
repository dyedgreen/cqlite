use super::*;
use crate::planner::{Filter, LoadProperty, MatchStep, UpdateStep};
use crate::store::PropRef;

#[test]
fn simplify_top_level_and() {
    let mut plan_before = QueryPlan {
        steps: vec![
            MatchStep::Filter(Filter::and(
                Filter::IsOrigin { edge: 0, node: 1 },
                Filter::IsOrigin { edge: 0, node: 1 },
            )),
            MatchStep::Filter(Filter::and(
                Filter::and(
                    Filter::IsOrigin { edge: 0, node: 1 },
                    Filter::IsOrigin { edge: 0, node: 1 },
                ),
                Filter::IsOrigin { edge: 0, node: 1 },
            )),
        ],
        updates: vec![],
        returns: vec![],
    };
    let plan_after = QueryPlan {
        steps: vec![
            MatchStep::Filter(Filter::IsOrigin { edge: 0, node: 1 }),
            MatchStep::Filter(Filter::IsOrigin { edge: 0, node: 1 }),
            MatchStep::Filter(Filter::IsOrigin { edge: 0, node: 1 }),
            MatchStep::Filter(Filter::IsOrigin { edge: 0, node: 1 }),
            MatchStep::Filter(Filter::IsOrigin { edge: 0, node: 1 }),
        ],
        updates: vec![],
        returns: vec![],
    };

    normalize::SplitTopLevelAnd::fix(&mut plan_before).unwrap();
    assert_eq!(plan_before, plan_after);
}

#[test]
fn canonicalize_check_node_label() {
    let mut plan_before = QueryPlan {
        steps: vec![
            MatchStep::LoadAnyNode { name: 0 },
            MatchStep::Filter(Filter::Eq(
                LoadProperty::Constant(PropRef::Text("LABEL")),
                LoadProperty::LabelOfNode { node: 0 },
            )),
            MatchStep::Filter(Filter::Eq(
                LoadProperty::LabelOfNode { node: 0 },
                LoadProperty::Constant(PropRef::Text("LABEL")),
            )),
        ],
        updates: vec![],
        returns: vec![],
    };
    let plan_after = QueryPlan {
        steps: vec![
            MatchStep::LoadAnyNode { name: 0 },
            MatchStep::Filter(Filter::NodeHasLabel {
                node: 0,
                label: "LABEL",
            }),
            MatchStep::Filter(Filter::NodeHasLabel {
                node: 0,
                label: "LABEL",
            }),
        ],
        updates: vec![],
        returns: vec![],
    };

    normalize::CanonicalizeCheckNodeLabel::apply(&mut plan_before).unwrap();
    assert_eq!(plan_before, plan_after);
}

#[test]
fn simplify_merge_sets() {
    let mut plan_before = QueryPlan {
        steps: vec![],
        updates: vec![
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "foo",
                value: LoadProperty::Parameter { name: "foo" },
            },
            UpdateStep::DeleteEdge { edge: 1 },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "foo",
                value: LoadProperty::Parameter { name: "bar" },
            },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "foo",
                value: LoadProperty::Parameter { name: "baz" },
            },
            UpdateStep::DeleteEdge { edge: 1 },
        ],
        returns: vec![],
    };
    let plan_after = QueryPlan {
        steps: vec![],
        updates: vec![
            UpdateStep::DeleteEdge { edge: 1 },
            UpdateStep::SetNodeProperty {
                node: 0,
                key: "foo",
                value: LoadProperty::Parameter { name: "baz" },
            },
        ],
        returns: vec![],
    };

    normalize::MergeDuplicateUpdates::apply(&mut plan_before).unwrap();
    assert_eq!(plan_before, plan_after);
}

#[test]
fn load_reorder_id_constrained_first() {
    let mut plan_before = QueryPlan {
        steps: vec![
            MatchStep::LoadAnyNode { name: 0 },
            MatchStep::LoadOriginEdge { name: 1, node: 0 },
            MatchStep::LoadAnyNode { name: 5 },
            MatchStep::LoadTargetNode { name: 2, edge: 1 },
            MatchStep::LoadEitherEdge { name: 3, node: 2 },
            MatchStep::LoadOtherNode {
                name: 4,
                edge: 3,
                node: 2,
            },
            MatchStep::LoadTargetEdge { name: 6, node: 5 },
            MatchStep::LoadOriginNode { name: 7, edge: 6 },
            MatchStep::Filter(Filter::NodeHasId {
                node: 4,
                id: LoadProperty::Parameter { name: "test" },
            }),
        ],
        updates: vec![],
        returns: vec![],
    };
    let plan_after = QueryPlan {
        steps: vec![
            MatchStep::LoadAnyNode { name: 4 },
            MatchStep::LoadEitherEdge { name: 3, node: 4 },
            MatchStep::LoadOtherNode {
                name: 2,
                edge: 3,
                node: 4,
            },
            MatchStep::LoadTargetEdge { name: 1, node: 2 },
            MatchStep::LoadOriginNode { name: 0, edge: 1 },
            MatchStep::LoadAnyNode { name: 5 },
            MatchStep::LoadTargetEdge { name: 6, node: 5 },
            MatchStep::LoadOriginNode { name: 7, edge: 6 },
            MatchStep::Filter(Filter::NodeHasId {
                node: 4,
                id: LoadProperty::Parameter { name: "test" },
            }),
        ],
        updates: vec![],
        returns: vec![],
    };

    loads::ReorderIdConstrainedFirst::apply(&mut plan_before).unwrap();
    assert_eq!(plan_before, plan_after);
}

#[test]
fn load_reorder_id_constrained_first_no_unnecessary_flips() {
    let plan = QueryPlan {
        steps: vec![
            MatchStep::LoadAnyNode { name: 0 },
            MatchStep::LoadOriginEdge { name: 1, node: 0 },
            MatchStep::LoadTargetNode { name: 2, edge: 1 },
            MatchStep::Filter(Filter::NodeHasId {
                node: 2,
                id: LoadProperty::Parameter { name: "foo" },
            }),
            MatchStep::Filter(Filter::NodeHasId {
                node: 0,
                id: LoadProperty::Parameter { name: "bar" },
            }),
        ],
        updates: vec![],
        returns: vec![],
    };

    let mut plan_copy = plan.clone();
    loads::ReorderIdConstrainedFirst::apply(&mut plan_copy).unwrap();
    assert_eq!(plan, plan_copy);
}

#[test]
fn load_any_node_to_load_exact_node() {
    let mut plan_before = QueryPlan {
        steps: vec![
            MatchStep::LoadAnyNode { name: 0 },
            MatchStep::Filter(Filter::NodeHasId {
                node: 0,
                id: LoadProperty::Parameter { name: "test" },
            }),
        ],
        updates: vec![],
        returns: vec![],
    };
    let plan_after = QueryPlan {
        steps: vec![MatchStep::LoadExactNode {
            name: 0,
            id: LoadProperty::Parameter { name: "test" },
        }],
        updates: vec![],
        returns: vec![],
    };

    loads::LoadAnyToLoadExact::apply(&mut plan_before).unwrap();
    assert_eq!(plan_before, plan_after);
}
