pub struct NavItem {
    pub key: &'static str,
    pub path: &'static str,
    pub label: &'static str,
}

pub const NAV_ITEMS: &[NavItem] = &[
    NavItem {
        key: "Home page",
        path: "/",
        label: "Home page",
    },
    NavItem {
        key: "processes",
        path: "/processes/upload",
        label: "Processes",
    },
    NavItem {
        key: "logout",
        path: "/logout",
        label: "Logout",
    },

];
