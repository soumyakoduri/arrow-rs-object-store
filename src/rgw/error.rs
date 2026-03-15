// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! Error handling and conversion for RGW backend
//!
//! This module maps errno codes returned by the RGW C API to object_store::Error variants.

use std::os::raw::c_int;

use crate::{Error, Path};

const STORE: &str = "RGW";

/// RGW-specific error types
#[derive(Debug, thiserror::Error)]
pub enum RgwError {
    /// RGW operation failed with errno code
    #[error("RGW operation failed with errno {0}")]
    Errno(c_int),

    /// Invalid UTF-8 in C string
    #[error("Invalid UTF-8 in C string")]
    InvalidUtf8,

    /// Null pointer returned from RGW
    #[error("Null pointer returned from RGW")]
    NullPointer,

    /// Required field is missing
    #[error("Required field is missing: {0}")]
    MissingField(&'static str),

    /// String contains null byte
    #[error("String contains null byte")]
    NulError(#[from] std::ffi::NulError),

    /// Task join error
    #[error("Background task failed")]
    JoinError(#[from] tokio::task::JoinError),
}

impl RgwError {
    /// Convert errno code to RgwError
    pub fn from_errno(errno: c_int) -> Self {
        Self::Errno(errno)
    }

    /// Check if this is a "not found" error (ENOENT = -2)
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Errno(-2))
    }

    /// Check if this is an "already exists" error (EEXIST = -17)
    pub fn is_already_exists(&self) -> bool {
        matches!(self, Self::Errno(-17))
    }

    /// Get errno code if this is an Errno variant
    pub fn errno(&self) -> Option<c_int> {
        match self {
            Self::Errno(code) => Some(*code),
            _ => None,
        }
    }
}

impl From<RgwError> for Error {
    fn from(err: RgwError) -> Self {
        match err {
            RgwError::Errno(errno) => errno_to_error(errno, Path::from("")),
            RgwError::InvalidUtf8 => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
            RgwError::NullPointer => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
            RgwError::MissingField(_) => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
            RgwError::NulError(_) => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
            RgwError::JoinError(_) => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

/// Convert errno code to object_store::Error
///
/// Maps common errno values to appropriate Error variants.
pub fn errno_to_error(errno: c_int, path: Path) -> Error {
    match -errno {
        // ENOENT (2): No such file or directory
        2 => Error::NotFound {
            path: path.to_string(),
            source: Box::new(RgwError::Errno(errno)),
        },

        // EEXIST (17): File exists
        17 => Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(RgwError::Errno(errno)),
        },

        // EINVAL (22): Invalid argument
        22 => Error::Generic {
            store: STORE,
            source: format!("Invalid argument (errno {errno})").into(),
        },

        // EACCES (13): Permission denied
        // EPERM (1): Operation not permitted
        1 | 13 => Error::Generic {
            store: STORE,
            source: format!("Permission denied (errno {errno})").into(),
        },

        // ENOMEM (12): Out of memory
        12 => Error::Generic {
            store: STORE,
            source: format!("Out of memory (errno {errno})").into(),
        },

        // EIO (5): I/O error
        5 => Error::Generic {
            store: STORE,
            source: format!("I/O error (errno {errno})").into(),
        },

        // ENOSYS (38): Function not implemented
        38 => Error::NotSupported {
            source: Box::new(RgwError::Errno(errno)),
        },

        // ENOTSUP (95): Operation not supported
        95 => Error::NotSupported {
            source: Box::new(RgwError::Errno(errno)),
        },

        // ETIMEDOUT (110): Connection timed out
        110 => Error::Generic {
            store: STORE,
            source: format!("Connection timed out (errno {errno})").into(),
        },

        // ECANCELED (125): Operation canceled
        125 => Error::Generic {
            store: STORE,
            source: format!("Operation canceled (errno {errno})").into(),
        },

        // All other errno codes
        _ => Error::Generic {
            store: STORE,
            source: format!("RGW operation failed with errno {errno}").into(),
        },
    }
}

/// Convert errno code to Result, with context
///
/// Returns Ok(()) if errno is 0, otherwise maps errno to Error with path context.
pub fn check_errno(errno: c_int, path: &Path) -> crate::Result<()> {
    if errno == 0 {
        Ok(())
    } else {
        Err(errno_to_error(errno, path.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_errno_not_found() {
        let err = errno_to_error(-2, Path::from("test/file.txt"));
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[test]
    fn test_errno_already_exists() {
        let err = errno_to_error(-17, Path::from("test/file.txt"));
        assert!(matches!(err, Error::AlreadyExists { .. }));
    }

    #[test]
    fn test_errno_not_supported() {
        let err = errno_to_error(-38, Path::from("test/file.txt"));
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    #[test]
    fn test_check_errno_success() {
        assert!(check_errno(0, &Path::from("test")).is_ok());
    }

    #[test]
    fn test_check_errno_failure() {
        assert!(check_errno(-2, &Path::from("test")).is_err());
    }

    #[test]
    fn test_rgw_error_is_not_found() {
        let err = RgwError::from_errno(-2);
        assert!(err.is_not_found());
    }

    #[test]
    fn test_rgw_error_is_already_exists() {
        let err = RgwError::from_errno(-17);
        assert!(err.is_already_exists());
    }
}
