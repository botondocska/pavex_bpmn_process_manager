use crate::telemetry;
use pavex::{Blueprint, blueprint::from, cookie::INJECT_RESPONSE_COOKIES};
use pavex_session::FINALIZE_SESSION;


/// The main blueprint, defining all the components used in this API.
pub fn blueprint() -> Blueprint {
    let mut bp = Blueprint::new();
    // Bring into scope constructors, error handlers, configuration
    // and prebuilt types defined in the following crates
    bp.import(from![
        // Local components, defined in this crate
        crate,
        // Components defined in the `pavex` crate,
        // by the framework itself.
        pavex,
        pavex_session,
        pavex_session_sqlx,
    ]);

    telemetry::instrument(&mut bp);
    bp.post_process(FINALIZE_SESSION);
    bp.post_process(INJECT_RESPONSE_COOKIES);
    
    bp.prefix("/api").routes(from![crate]);
    bp
}
