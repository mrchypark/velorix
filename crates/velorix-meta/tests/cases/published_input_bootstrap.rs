use super::*;
use velorix_meta::{
    PublishedInputBootstrapCursorV1, PublishedInputBootstrapProgressV1, ViewBootstrapControlV1,
};

#[tokio::test]
async fn pending_derived_bootstrap_requires_complete_bound_monotone_progress() {
    let store = InMemoryMetaStore::default();
    let mut request = orders_view_bootstrap_request();
    request.relations.clear();
    let mut edge = view_input_edge("baseline-edge");
    edge.output_schema_hash = format!("sha256:{}", "c".repeat(64));
    request.view_inputs = vec![edge.clone()];
    let control = match store.begin_view_bootstrap(request).await.unwrap() {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("{other:?}"),
    };
    let owner = acquire_owner_for_scope(&store, "orders-view", "orders-view", "owner-a").await;
    let mut pointer = checkpoint_pointer_for_cut(
        1,
        "a",
        control.bootstrap_generation,
        &control.plan_hash,
        &control.bootstrap_cut,
    );
    let publish = |candidate, previous| PublishStandingRuntimeCheckpointRequest {
        candidate,
        expected_previous: previous,
        owner: owner.clone(),
        expected_relation_source_cuts: None,
    };
    let promote = |checkpoint| PromoteViewBootstrapRequest {
        tenant_id: control.tenant_id.clone(),
        program_id: control.program_id.clone(),
        view_id: control.view_id.clone(),
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        owner: owner.clone(),
        checkpoint,
    };
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(pointer.clone(), None))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert!(matches!(
        store
            .fix_view_bootstrap_activation_cut(FixViewBootstrapActivationCutRequest {
                tenant_id: control.tenant_id.clone(),
                program_id: control.program_id.clone(),
                view_id: control.view_id.clone(),
                bootstrap_generation: control.bootstrap_generation,
                plan_hash: control.plan_hash.clone(),
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        FixViewBootstrapActivationCutOutcome::Fixed(_)
    ));
    assert_eq!(
        store
            .promote_view_bootstrap(promote(pointer.clone()))
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Conflict
    );
    let progress = PublishedInputBootstrapProgressV1 {
        schema_version: 1,
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        inputs: vec![PublishedInputBootstrapCursorV1 {
            input_edge: edge.edge_id.clone(),
            captured_cursor: edge.bootstrap_cursor.clone().unwrap(),
            output_manifest_ref: "captured-immutable-manifest".into(),
            output_manifest_hash: format!("sha256:{}", "b".repeat(64)),
            output_schema_hash: edge.output_schema_hash.clone(),
            total_pages: 2,
            total_rows: 3,
            next_page_index: 1,
            applied_rows: 2,
        }],
    };
    let mut partial = checkpoint_pointer_for_cut(
        2,
        "b",
        control.bootstrap_generation,
        &control.plan_hash,
        &control.bootstrap_cut,
    );
    bind_checkpoint_predecessor(&mut partial, &pointer);
    partial.published_input_bootstrap = Some(progress.clone());
    partial.input_coverage = None;
    partial.coverage_hash.clear();
    let mut false_coverage = partial.clone();
    false_coverage.input_coverage = pointer.input_coverage.clone();
    false_coverage.coverage_hash = pointer.coverage_hash.clone();
    assert!(store
        .publish_standing_runtime_checkpoint(publish(false_coverage, Some(pointer.clone())))
        .await
        .is_err());
    let mut wrong_generation = partial.clone();
    wrong_generation
        .published_input_bootstrap
        .as_mut()
        .unwrap()
        .bootstrap_generation += 1;
    assert!(store
        .publish_standing_runtime_checkpoint(publish(wrong_generation, Some(pointer.clone())))
        .await
        .is_err());
    let mut mismatched = partial.clone();
    mismatched
        .published_input_bootstrap
        .as_mut()
        .unwrap()
        .inputs[0]
        .captured_cursor
        .output_epoch += 1;
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(mismatched, Some(pointer.clone())))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Conflict
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(partial.clone(), Some(pointer.clone())))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(partial.clone(), Some(pointer)))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Duplicate
    );
    assert_eq!(
        store
            .promote_view_bootstrap(promote(partial.clone()))
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Conflict
    );
    pointer = checkpoint_pointer_for_cut(
        3,
        "c",
        control.bootstrap_generation,
        &control.plan_hash,
        &control.bootstrap_cut,
    );
    bind_checkpoint_predecessor(&mut pointer, &partial);
    pointer.published_input_bootstrap = Some(progress);
    let complete_coverage = pointer.input_coverage.take();
    let complete_coverage_hash = std::mem::take(&mut pointer.coverage_hash);
    for mutation in 0..3 {
        let mut bad = pointer.clone();
        let input = &mut bad.published_input_bootstrap.as_mut().unwrap().inputs[0];
        match mutation {
            0 => {
                input.next_page_index = 0;
                input.applied_rows = 0;
            }
            1 => input.output_manifest_ref.push_str("-different"),
            2 => input.total_rows += 1,
            _ => unreachable!(),
        }
        assert_eq!(
            store
                .publish_standing_runtime_checkpoint(publish(bad, Some(partial.clone())))
                .await
                .unwrap(),
            PublishStandingRuntimeCheckpointOutcome::Conflict
        );
    }
    let input = &mut pointer.published_input_bootstrap.as_mut().unwrap().inputs[0];
    input.next_page_index = 2;
    input.applied_rows = 3;
    pointer.input_coverage = complete_coverage;
    pointer.coverage_hash = complete_coverage_hash;
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(pointer.clone(), Some(partial)))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert!(matches!(
        store
            .promote_view_bootstrap(promote(pointer.clone()))
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Promoted(_)
    ));
    assert!(matches!(
        store
            .promote_view_bootstrap(promote(pointer.clone()))
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Duplicate(_)
    ));
    let mut later = checkpoint_pointer_for_cut(
        4,
        "d",
        control.bootstrap_generation,
        &control.plan_hash,
        &control.bootstrap_cut,
    );
    bind_checkpoint_predecessor(&mut later, &pointer);
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(later.clone(), Some(pointer.clone())))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Conflict
    );
    later.published_input_bootstrap = pointer.published_input_bootstrap.clone();
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(publish(later, Some(pointer)))
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
}

#[tokio::test]
async fn deferred_edge_remains_declared_and_capture_does_not_change_declaration() {
    let store = InMemoryMetaStore::default();
    let mut request = orders_view_bootstrap_request();
    request.relations.clear();
    let mut edge = view_input_edge("deferred-edge");
    edge.bootstrap_cursor = None;
    request.view_inputs = vec![edge];
    let control = match store.begin_view_bootstrap(request.clone()).await.unwrap() {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(control.view_inputs.len(), 1);
    assert!(control.view_inputs[0].bootstrap_cursor.is_none());
    assert!(control.bootstrap_cut.relations.is_empty());
    let wire = serde_json::to_value(&control).unwrap();
    assert!(wire["view_inputs"][0].get("bootstrap_cursor").is_none());
    let restored: ViewBootstrapControlV1 = serde_json::from_value(wire).unwrap();
    assert_eq!(control, restored);
    assert!(matches!(
        store.begin_view_bootstrap(request).await.unwrap(),
        BeginViewBootstrapOutcome::Duplicate(_)
    ));
}
