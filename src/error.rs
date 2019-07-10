use std::{error::Error, fmt, io, time};

pub struct DiskioError(pub String);

impl fmt::Display for DiskioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<io::Error> for DiskioError {
    fn from(err: io::Error) -> DiskioError {
        DiskioError(err.description().to_string())
    }
}

impl From<time::SystemTimeError> for DiskioError {
    fn from(err: time::SystemTimeError) -> DiskioError {
        DiskioError(err.description().to_string())
    }
}
