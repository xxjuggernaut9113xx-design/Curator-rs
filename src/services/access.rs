//! Caller authority carried into shared operations independently of HTTP.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Actor {
    LocalOwner,
    RemoteViewer,
}

impl Actor {
    pub fn can_edit_library(self) -> bool {
        matches!(self, Self::LocalOwner)
    }
}
