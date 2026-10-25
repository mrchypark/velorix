use super::*;

/// Continue only requests whose caller expects a complete demand result.
pub(super) async fn post(router: &Router, path: &str, body: Value, expected: StatusCode) -> Value {
    let may_continue = expected == StatusCode::OK
        && (path.ends_with("/backfill") || path.ends_with("/query"))
        && body.get("action").is_none_or(|action| action == "step");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    for _ in 0..64 {
        let (status, value) = tokio::time::timeout_at(
            deadline,
            call_json(router, Method::POST, path, body.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("{path}: demand continuation exceeded its total deadline"));
        if may_continue && status == StatusCode::ACCEPTED {
            let progress = &value["materialization"];
            assert_eq!(progress["complete"], false, "{path}: {value}");
            assert!(value.get("rows").is_none(), "partial rows escaped: {value}");
            assert!(
                matches!(
                    progress["status"].as_str(),
                    Some("dependency_pending" | "owner_unavailable_pending" | "incomplete")
                ),
                "{path}: unexpected pending response: {value}"
            );
            assert!(
                progress["remaining_rows"]
                    .as_u64()
                    .is_some_and(|rows| rows > 0),
                "{path}: pending response has no remaining work: {value}"
            );
            tokio::task::yield_now().await;
            continue;
        }
        assert_eq!(status, expected, "{path}: {value}");
        return value;
    }
    panic!("{path}: demand did not complete within 64 requests");
}
