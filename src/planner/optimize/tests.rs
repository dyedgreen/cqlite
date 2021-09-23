use super::*;
use crate::planner::{Filter, LoadProperty, MatchStep, UpdateStep};

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

    normalize::SplitTopLevelAnd::fix(&mut &mut plan_before).unwrap();
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
        ],
        returns: vec![],
    };
    let plan_after = QueryPlan {
        steps: vec![],
        updates: vec![UpdateStep::SetNodeProperty {
            node: 0,
            key: "foo",
            value: LoadProperty::Parameter { name: "baz" },
        }],
        returns: vec![],
    };

    normalize::MergeDuplicateUpdates::fix(&mut &mut plan_before).unwrap();
    assert_eq!(plan_before, plan_after);
}
