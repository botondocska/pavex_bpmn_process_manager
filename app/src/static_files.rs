use pavex::Response;
use pavex::get;
use pavex::http::header::{CONTENT_TYPE, HeaderValue};

#[get(path = "/static/output.css")]
pub fn output_css() -> Response {
    Response::ok()
        .set_typed_body(include_str!("../static/output.css"))
        .insert_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/css; charset=utf-8"),
        )
}
