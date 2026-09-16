use crate::helpers::TestApi;
use pavex::http::StatusCode;
use uuid::Uuid;

#[tokio::test]
async fn signup_succeeds_with_valid_data() {
    let api = TestApi::spawn().await;
    let email = format!("{}@example.com", Uuid::new_v4());

    let response = api.post_signup(&email, "super_secret_password").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("hx-redirect"));
}

#[tokio::test]
async fn signup_fails_with_duplicate_email() {
    let api = TestApi::spawn().await;
    let email = format!("{}@example.com", Uuid::new_v4());

    let first = api.post_signup(&email, "super_secret_password").await;
    assert_eq!(first.status(), StatusCode::OK);

    let second = api.post_signup(&email, "another_password").await;
    assert_eq!(second.status(), StatusCode::CONFLICT);
}
