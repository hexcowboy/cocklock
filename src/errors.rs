use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CockLockError {
    CertificateFileError(std::io::Error, String),
    NativeTlsError(native_tls::Error, String),
    PostgresError(postgres::Error),
    NoClients,
    NotAvailable,
}

impl Display for CockLockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CockLockError::CertificateFileError(err, cert_file_path) => {
                write!(
                    f,
                    "Error opening certificate file: {cert_file_path:?}: {err:?}",
                )
            }
            CockLockError::NativeTlsError(err, cert_file_path) => {
                write!(
                    f,
                    "Error when creating a new TLS certificate: {cert_file_path:?}: {err:?}",
                )
            }
            CockLockError::PostgresError(err) => {
                write!(f, "Error connecting to client: {err:?}")
            }
            CockLockError::NoClients => {
                write!(f, "No clients provided to CockLock")
            }
            CockLockError::NotAvailable => {
                write!(f, "The namespace is already locked")
            }
        }
    }
}

impl std::error::Error for CockLockError {}

impl From<postgres::Error> for CockLockError {
    fn from(err: postgres::Error) -> Self {
        CockLockError::PostgresError(err)
    }
}
