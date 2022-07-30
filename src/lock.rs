use postgres::Client;

use crate::builder::CockLockBuilder;

/// The lock manager
///
/// Implements the necessary functionality to acquire and release locks
/// and handles the Postgres/Cockroach connections
pub struct CockLock {
    /// List of all Postgres/Cockroach clients
    pub clients: Vec<Client>,
}

impl CockLock {
    pub fn builder() -> CockLockBuilder {
        CockLockBuilder::default()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
