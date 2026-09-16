use pavex_session::Session;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Theme {
    Forest,
    Nord,
}

impl Theme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Theme::Forest => "forest",
            Theme::Nord => "nord",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "nord" => Theme::Nord,
            _ => Theme::Forest,
        }
    }
}
#[pavex::methods]
impl Theme {
    #[request_scoped]
    pub async fn extract(session: &Session<'_>) -> Self {
        match session.get::<String>("theme").await {
            Ok(Some(raw)) => Theme::parse(&raw),
            _ => Theme::Forest,
        }
    }
}
