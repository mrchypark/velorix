use velorix_core::query::QueryPolicyError;

#[test]
fn query_policy_scan_file_limit_has_typed_error() {
    let error = QueryPolicyError::ScanFilesExceeded {
        observed_files: 2,
        max_files: 1,
    };

    assert_eq!(
        error.to_string(),
        "query would scan 2 files, above query policy limit of 1 files"
    );
}

#[test]
fn query_policy_scan_byte_limit_has_typed_error() {
    let error = QueryPolicyError::ScanBytesExceeded {
        observed_bytes: 2048,
        max_bytes: 1024,
    };

    assert_eq!(
        error.to_string(),
        "query would scan 2048 bytes, above query policy limit of 1024 bytes"
    );
}

#[test]
fn query_policy_object_request_limit_has_typed_error() {
    let error = QueryPolicyError::ObjectRequestsExceeded {
        observed_requests: 3,
        max_requests: 2,
    };

    assert_eq!(
        error.to_string(),
        "query would issue at least 3 object requests, above query policy limit of 2 object requests"
    );
}
