use postgres::error::SqlState;
use postgres::Client;
use uuid::Uuid;

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
    pub(crate) id: Uuid,
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
    pub fn lock<T: ToString>(
        &mut self,
        lock_name: T,
        timeout_ms: i32,
    ) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            let result = client.execute(
                &self.queries.lock,
                &[&self.id, &lock_name.to_string(), &timeout_ms],
            );

            match result {
                Err(err) => {
                    if err.is_closed()
                        || err.code() == Some(&SqlState::ADMIN_SHUTDOWN)
                        || err.code() == Some(&SqlState::CRASH_SHUTDOWN)
                    {
                        continue;
                    } else {
                        return Err(CockLockError::PostgresError(err));
                    }
                }
                Ok(row_count) => {
                    if row_count == 0 {
                        return Err(CockLockError::NotAvailable);
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        // This is only reached if every client returned ClientNotAvailable
        Err(CockLockError::NoClientsAvailable)
    }

    /// Try to release the lock on all clients
    pub fn unlock<T: ToString>(&mut self, lock_name: T) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            let result = client.execute(&self.queries.unlock, &[&self.id, &lock_name.to_string()]);

            match result {
                Err(err) => {
                    if err.is_closed()
                        || err.code() == Some(&SqlState::ADMIN_SHUTDOWN)
                        || err.code() == Some(&SqlState::CRASH_SHUTDOWN)
                    {
                        continue;
                    } else {
                        return Err(CockLockError::PostgresError(err));
                    }
                }
                Ok(row_count) => {
                    if row_count == 0 {
                        return Err(CockLockError::NotAvailable);
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        // This is only reached if every client returned ClientNotAvailable
        Err(CockLockError::NoClientsAvailable)
    }

    /// Remove the tables and functions that were created by CockLock
    pub fn clean_up(&mut self) -> Result<(), CockLockError> {
        for client in self.clients.iter_mut() {
            client.batch_execute(&self.queries.clean_up)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use testcontainers::{clients, images::postgres::Postgres, Container, RunnableImage};
    use uuid::Uuid;

    use crate::{errors::CockLockError, CockLock};

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
    fn lock_works() {
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

        let mut cock_lock_alice = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        let mut cock_lock_bob = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        // Assert both Bob and Alice can create unique locks
        assert!(cock_lock_alice.lock(Uuid::new_v4(), 1_000).is_ok());
        assert!(cock_lock_bob.lock(Uuid::new_v4(), 1_000).is_ok());

        // Assert Bob cannot create a lock that Alice has acquired
        let lock_name = Uuid::new_v4();
        assert!(cock_lock_alice.lock(lock_name, 10_000).is_ok());
        assert!(!cock_lock_bob.lock(lock_name, 10_000).is_ok());

        // Assert Bob's lease can extend if it's already acquired by him
        let lock_name = Uuid::new_v4();
        assert!(cock_lock_bob.lock(lock_name, 10_000).is_ok());
        assert!(cock_lock_bob.lock(lock_name, 10_000).is_ok());
    }

    #[test]
    fn unlock_works() {
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

        let mut cock_lock_alice = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        let mut cock_lock_bob = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        // Assert both Bob and Alice can create unique locks and unlock them
        let alice_lock = Uuid::new_v4();
        assert!(cock_lock_alice.lock(alice_lock, 1_000).is_ok());
        assert!(cock_lock_alice.unlock(alice_lock).is_ok());
        let bob_lock = Uuid::new_v4();
        assert!(cock_lock_bob.lock(bob_lock, 1_000).is_ok());
        assert!(cock_lock_bob.unlock(bob_lock).is_ok());

        // Assert Bob cannot unlock Alice's lock
        let alice_lock = Uuid::new_v4();
        assert!(cock_lock_alice.lock(alice_lock, 10_000).is_ok());
        assert!(!cock_lock_bob.unlock(alice_lock).is_ok());

        // Assert a lock cannot be unlocked twice
        let bob_lock = Uuid::new_v4();
        assert!(cock_lock_bob.lock(bob_lock, 10_000).is_ok());
        assert!(cock_lock_bob.unlock(bob_lock).is_ok());
        assert!(!cock_lock_bob.unlock(bob_lock).is_ok());
    }

    #[test]
    fn error_on_connection_drop() {
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

        let mut cock_lock = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        for node in nodes {
            node.stop();
        }

        let result = cock_lock.lock("test", 1);
        assert!(result.is_err());
        let is_correct_error = match result {
            Err(CockLockError::NoClientsAvailable) => true,
            _ => false,
        };
        assert!(is_correct_error);
    }

    #[test]
    fn cleanup_works() {
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

        let mut cock_lock = CockLock::builder()
            .with_connection_strings(connection_strings.clone())
            .build()
            .unwrap();

        assert!(cock_lock.clean_up().is_ok());

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
            assert!(!exists);
        }
    }
}
