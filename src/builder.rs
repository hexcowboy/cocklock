use std::fs;

use native_tls::{Certificate, TlsConnector};
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use uuid::Uuid;

use crate::errors::CockLockError;
use crate::lock::{CockLock, CockLockQueries, DEFAULT_TABLE};
use crate::queries::*;

pub struct CockLockBuilder {
    /// List of all Postgres/Cockroach clients
    clients: Vec<Client>,
    client_connection_strings: Vec<String>,
    tls_connector: Option<MakeTlsConnector>,
    table_name: String,
}

impl Default for CockLockBuilder {
    fn default() -> Self {
        Self {
            clients: vec![],
            client_connection_strings: vec![],
            tls_connector: None,
            table_name: DEFAULT_TABLE.to_owned(),
        }
    }
}

/// A builder for the CockLock struct
///
/// Allows chaining of methods to build a new CockLock using either Postgres
/// or Cockroach connections.
impl CockLockBuilder {
    /// Instantiate a new CockLock builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add some client connection strings
    pub fn with_connection_strings(mut self, connection_strings: Vec<&str>) -> Self {
        for connection_string in connection_strings {
            self.client_connection_strings
                .push(connection_string.to_owned());
        }
        self
    }

    /// Change the table name to be used for locks
    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = table_name;
        self
    }

    /// Add a TLS certification which will be applied to all connections
    pub fn with_cert(mut self, path_to_cert: &str) -> Result<Self, CockLockError> {
        let cert = match fs::read(path_to_cert) {
            Ok(cert) => cert,
            Err(err) => {
                return Err(CockLockError::CertificateFileError(
                    err,
                    path_to_cert.to_owned(),
                ))
            }
        };
        let cert = match Certificate::from_pem(&cert) {
            Ok(cert) => cert,
            Err(err) => return Err(CockLockError::NativeTlsError(err, path_to_cert.to_owned())),
        };
        let connector = match TlsConnector::builder().add_root_certificate(cert).build() {
            Ok(connector) => connector,
            Err(err) => return Err(CockLockError::NativeTlsError(err, path_to_cert.to_owned())),
        };
        let connector = MakeTlsConnector::new(connector);

        self.tls_connector = Some(connector);
        Ok(self)
    }

    /// Add custom clients
    ///
    /// Clients may be made from the postgres package and added here
    pub fn with_clients(mut self, clients: &mut Vec<Client>) -> Self {
        self.clients.append(clients);
        self
    }

    /// Build a CockLock instance using the builder
    pub fn build(self) -> Result<CockLock, CockLockError> {
        let mut clients = self.clients;
        match self.tls_connector {
            Some(connector) => {
                for connection_string in self.client_connection_strings {
                    clients.push(Client::connect(&connection_string, connector.clone())?);
                }
            }
            None => {
                for connection_string in self.client_connection_strings {
                    clients.push(Client::connect(&connection_string, NoTls)?);
                }
            }
        }

        if clients.is_empty() {
            return Err(CockLockError::NoClients);
        }

        let instance = CockLock::new(CockLock {
            id: Uuid::new_v4().to_string(),
            clients,
            table_name: self.table_name.clone(),
            queries: CockLockQueries {
                create_table: PG_TABLE_QUERY.replace("TABLE_NAME", &self.table_name),
                lock: PG_TABLE_QUERY.replace("TABLE_NAME", &self.table_name),
                unlock: PG_TABLE_QUERY.replace("TABLE_NAME", &self.table_name),
                clean_up: PG_TABLE_QUERY.replace("TABLE_NAME", &self.table_name),
            },
        })?;

        Ok(instance)
    }
}
