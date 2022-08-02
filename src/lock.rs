use postgres::error::SqlState;
use postgres::Client;

use crate::builder::CockLockBuilder;
use crate::errors::CockLockError;
use crate::queries::*;

pub static DEFAULT_TABLE: &str = "_locks";

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

        for client in instance.clients.iter_mut() {
            client.execute(
                &PG_TABLE_QUERY.replace("TABLE_NAME", &instance.table_name),
                &[&instance.table_name],
            )?;
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
            let result = client.execute(
                &PG_LOCK_QUERY.replace("TABLE_NAME", &self.table_name),
                &[&self.id, &lock_name, &timeout_ms],
            );

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
            client.execute(
                &PG_UNLOCK_QUERY.replace("TABLE_NAME", &self.table_name),
                &[&self.id, &lock_name],
            )?;
        }

        Ok(())
    }

    /// Remove the tables and functions that were created by CockLock
    pub fn clean_up(&mut self) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            client.execute(&PG_CLEAN_UP.replace("TABLE_NAME", &self.table_name), &[])?;
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
