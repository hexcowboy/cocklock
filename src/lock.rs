use postgres::error::SqlState;
use postgres::Client;

use crate::builder::CockLockBuilder;
use crate::errors::CockLockError;
use crate::queries::*;

pub static DEFAULT_TABLE: &str = "_locks";

#[derive(Default)]
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
            lock: PG_LOCK_QUERY.replace("TABLE_NAME", &instance.table_name),
            unlock: PG_UNLOCK_QUERY.replace("TABLE_NAME", &instance.table_name),
            clean_up: PG_CLEAN_UP_QUERY.replace("TABLE_NAME", &instance.table_name),
        };

        for client in instance.clients.iter_mut() {
            client.batch_execute(&instance.queries.create_table)?;
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
    use testcontainers::{clients, images::postgres::Postgres, Container, RunnableImage};

    use crate::CockLock;

    #[test]
    fn new_creates_tables() {
        let docker = clients::Cli::default();
        let nodes: Vec<Container<Postgres>> = (1..=3)
            .map(|_| {
                let image = RunnableImage::from(Postgres::default()).with_tag("14-alpine");
                docker.run(image)
            })
            .collect();

        let connection_strings: Vec<String> = nodes
            .iter()
            .map(|node| {
                format!(
                    "postgres://postgres:postgres@127.0.0.1:{}/postgres",
                    node.get_host_port_ipv4(5432)
                )
            })
            .collect();

        let cock_lock = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        for connection_string in connection_strings {
            let mut conn = postgres::Client::connect(&connection_string, postgres::NoTls).unwrap();
            let row = conn
                .query_one(
                    "
                    select exists (
                        select from information_schema.tables
                        where table_name = $1
                    );
                    ",
                    &[&cock_lock.table_name],
                )
                .unwrap();
            let exists: bool = row.get("exists");
            assert!(exists);
        }
    }

    #[test]
    fn tls_works() {}

    #[test]
    fn lock_works() {}

    #[test]
    fn unlock_works() {}

    #[test]
    fn cleanup_works() {}
}
