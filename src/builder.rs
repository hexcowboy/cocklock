use std::fs;

use native_tls::{Certificate, TlsConnector};
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;

use crate::errors::CockLockError;
use crate::lock::CockLock;

#[derive(Default)]
pub struct CockLockBuilder {
    /// List of all Postgres/Cockroach clients
    clients: Vec<Client>,
    client_connection_strings: Vec<String>,
    tls_connector: Option<MakeTlsConnector>,
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
    pub fn with_connection_strings(mut self, connection_strings: Vec<String>) -> Self {
        for connection_string in connection_strings {
            self.client_connection_strings.push(connection_string);
        }
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

    /// Build a CockLock struct using the builder
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

        Ok(CockLock { clients: vec![] })
    }
}
