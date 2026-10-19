use super::*;

#[tokio::test]
async fn demand_scope_becomes_conservative_native_page_pruning_predicate() {
    let (_, active) = native_output_pages::tests::public_native_fixture().await;
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == "purchases_by_user")
        .unwrap();
    let safe_scope = demand_materialization::DemandScope {
        predicate: "id >= 'm'".into(),
    };
    let predicate = demand_scope_page_predicate(
        Some(&safe_scope),
        "SELECT * FROM purchases_by_user",
        &[],
        schema,
    );
    assert!(matches!(
        predicate,
        Some(velorix_core::query::PagePredicate::Compare {
            column,
            op: velorix_core::query::PageComparison::Ge,
            value: velorix_core::query::PageScalar::Utf8(ref value),
        }) if column == "id" && value == "m"
    ));
    assert_eq!(
        demand_scope_page_predicate(None, "SELECT * FROM purchases_by_user", &[], schema),
        None,
        "a query without a residual filter can page without scanning all output pages"
    );
    let unsupported = demand_materialization::DemandScope {
        predicate: "abs(power) > 0".into(),
    };
    assert_eq!(
        demand_scope_page_predicate(
            Some(&unsupported),
            "SELECT * FROM purchases_by_user",
            &[],
            schema
        ),
        Some(velorix_core::query::PagePredicate::Unknown),
        "an unproven scope must scan every page"
    );
    assert_eq!(
        demand_scope_page_predicate(
            None,
            "SELECT * FROM purchases_by_user WHERE abs(power) > 0",
            &[],
            schema
        ),
        Some(velorix_core::query::PagePredicate::Unknown),
        "an unproven SQL filter must scan every page"
    );
}

#[tokio::test]
async fn explicit_materialization_rejects_legacy_and_pinned_queries() {
    let (mut state, _) = native_output_pages::tests::public_native_fixture().await;
    let mut request: QueryViewRequest = serde_json::from_value(json!({})).unwrap();
    state.relation_ingest_mode = RelationIngestMode::Legacy;
    assert!(validate_explicit_materialization(&state, &request, false).is_ok());
    let error = validate_explicit_materialization(&state, &request, true).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("authoritative"));
    state.relation_ingest_mode = RelationIngestMode::Authoritative;
    assert!(validate_explicit_materialization(&state, &request, true).is_ok());
    request.epoch = Some(1);
    assert_eq!(
        validate_explicit_materialization(&state, &request, true)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
    request.epoch = None;
    request.page_token = Some("pinned-result".into());
    assert_eq!(
        validate_explicit_materialization(&state, &request, true)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
    assert!(validate_explicit_materialization(&state, &request, false).is_ok());
}

#[tokio::test]
async fn query_response_exhausted_limiter_rejects_before_native_reads_and_holds_through_encoding() {
    let (mut state, mut active) = native_output_pages::tests::public_native_fixture().await;
    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    state
        .query_policy_catalog()
        .unwrap()
        .create_for_production_table_scan("default", "single-query", policy)
        .await
        .unwrap();
    active.api.as_mut().unwrap().query_policy_id = Some("single-query".into());
    let limiter = state
        .query_limiter_for_policy("single-query", policy)
        .unwrap()
        .unwrap();
    let permit = acquire_query_permit(policy, Some(&limiter)).unwrap();
    let counts = crate::tests::ObjectStoreAccessCounts::default();
    state.store = Arc::new(crate::tests::CountingObjectStore::new(
        state.store.clone(),
        counts.clone(),
    ));
    let query = |state: ApiState, active: ActiveMaterializedView| {
        query_active_view_output_batches_impl(
            state,
            active,
            None,
            Some("SELECT * FROM purchases_by_user WHERE id='m'".into()),
            BTreeMap::new(),
            SnapshotPageRequest::default(),
            true,
        )
    };
    let result = query(state.clone(), active.clone()).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("exhausted limiter admitted a query"),
    };
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("concurrent"));
    assert!(!counts
        .get_paths()
        .iter()
        .any(|path| path.starts_with("v1/standing-runtime-")));
    drop(permit);
    let result = query(state, active).await.unwrap();
    assert!(
        acquire_query_permit(policy, Some(&limiter)).is_err(),
        "permit must cover result encoding"
    );
    let response = result.into_http(QueryResponseFormat::Arrow).unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        acquire_query_permit(policy, Some(&limiter)).is_ok(),
        "completed encoding must release the permit"
    );
}
