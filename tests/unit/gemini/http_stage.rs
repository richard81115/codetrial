use super::*;

/// The stage an answer's body failure is logged under says what failed: the
/// size cap, a body that would not decode, or the read itself, a reset or a
/// timeout, which is not a decode failure however it was read.
#[tokio::test]
async fn a_body_failure_is_named_for_what_failed() {
    let over: Box<dyn std::error::Error + Send + Sync> =
        io::Error::new(io::ErrorKind::InvalidData, "too long").into();
    assert_eq!(body_stage(over.as_ref()), "body_limit");

    let undecodable = reqwest::Response::from(axum::http::Response::new("not json"))
        .json::<Value>()
        .await
        .unwrap_err();
    assert_eq!(body_stage(&undecodable), "decode");

    let reset: Box<dyn std::error::Error + Send + Sync> = "connection reset".into();
    assert_eq!(body_stage(reset.as_ref()), "body_read");
}
