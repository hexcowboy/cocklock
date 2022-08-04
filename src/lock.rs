use postgres::error::SqlState;
use postgres::Client;

use crate::builder::CockLockBuilder;
use crate::errors::CockLockError;
use crate::queries::*;

pub static DEFAULT_TABLE: &str = "_locks";

pub(crate) struct CockLockQueries {
    pub create_table: String,
    pub lock: String,
    pub unlock: String,
    pub clean_up: String,
}

/// The lock manager
///
/// Implements the necessary functionality to acquire and release locks
/// and handles the Postgres/Cockroach connections
pub struct CockLock {
    /// The unique ID of the CockLock instance
    pub(crate) id: String,
    /// List of all Postgres/Cockroach clients
    pub clients: Vec<Client>,
    pub table_name: String,
    pub(crate) queries: CockLockQueries,
}

impl CockLock {
    /// Get a builder object to easily and semantically create a new instance
    pub fn builder() -> CockLockBuilder {
        CockLockBuilder::default()
    }

    /// Create a new instance with pre-existing client objects
    ///
    /// This method will create a new table called `_locks` on each of the
    /// clients, skipping if the table already exists
    pub fn new(cock_lock: CockLock) -> Result<Self, CockLockError> {
        let mut instance = cock_lock;

        instance.queries = CockLockQueries {
            create_table: PG_TABLE_QUERY.replace("TABLE_NAME", &instance.table_name),
            lock: PG_TABLE_QUERY.replace("TABLE_NAME", &instance.table_name),
            unlock: PG_TABLE_QUERY.replace("TABLE_NAME", &instance.table_name),
            clean_up: PG_TABLE_QUERY.replace("TABLE_NAME", &instance.table_name),
        };

        for client in instance.clients.iter_mut() {
            client.execute(&instance.queries.create_table, &[&instance.table_name])?;
        }

        Ok(instance)
    }

    /// Try to create a new lock on all clients
    ///
    /// Returns Ok(()) if successful or a custom CockLockError::AlreadyLocked
    /// error when the lock is not available.
    ///
    /// Pass 0 to `timeout_ms` to provide an infinite timeout (locked until
    /// explicitly unlocked).
    ///
    /// If the lock is already acquired by the instance, calling this function
    /// simply overrides the timeout on the lock.
    pub fn lock(&mut self, lock_name: &str, timeout_ms: u32) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            let result = client.execute(&self.queries.lock, &[&self.id, &lock_name, &timeout_ms]);

            if let Err(err) = result {
                match err.code() {
                    _ if err.code() == Some(&SqlState::from_code("23505")) => {
                        return Err(CockLockError::NotAvailable)
                    }
                    _ => return Err(CockLockError::PostgresError(err)),
                };
            }
        }

        Ok(())
    }

    /// Try to release the lock on all clients
    pub fn unlock(&mut self, lock_name: &str) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            client.execute(&self.queries.unlock, &[&self.id, &lock_name])?;
        }

        Ok(())
    }

    /// Remove the tables and functions that were created by CockLock
    pub fn clean_up(&mut self) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            client.execute(&self.queries.clean_up, &[])?;
        }

        Ok(())
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
