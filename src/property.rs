use crate::store::PropOwned;
use crate::Error;
use std::convert::{TryFrom, TryInto};

/// A single property which can be stored on a node or edge.
#[derive(Debug, Clone, PartialEq)]
pub enum Property {
    Id(u64),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl PropOwned {
    pub(crate) fn to_external(self) -> Property {
        match self {
            Self::Id(id) => Property::Id(id),
            Self::Integer(num) => Property::Integer(num),
            Self::Real(num) => Property::Real(num),
            Self::Boolean(val) => Property::Boolean(val),
            Self::Text(text) => Property::Text(text),
            Self::Blob(bytes) => Property::Blob(bytes),
            Self::Null => Property::Null,
        }
    }
}

impl Property {
    pub(crate) fn to_internal(self) -> PropOwned {
        match self {
            Self::Id(id) => PropOwned::Id(id),
            Self::Integer(num) => PropOwned::Integer(num),
            Self::Real(num) => PropOwned::Real(num),
            Self::Boolean(val) => PropOwned::Boolean(val),
            Self::Text(text) => PropOwned::Text(text),
            Self::Blob(bytes) => PropOwned::Blob(bytes),
            Self::Null => PropOwned::Null,
        }
    }
}

macro_rules! try_from {
    ($type:ty, $variant:ident) => {
        impl TryFrom<Property> for $type {
            type Error = Error;

            fn try_from(value: Property) -> Result<Self, Self::Error> {
                match value {
                    Property::$variant(val) => Ok(val),
                    _ => Err(Error::TypeMismatch),
                }
            }
        }

        impl TryFrom<Property> for Option<$type> {
            type Error = Error;

            fn try_from(value: Property) -> Result<Self, Self::Error> {
                match value {
                    Property::Null => Ok(None),
                    prop => Ok(Some(prop.try_into()?)),
                }
            }
        }

        impl From<$type> for Property {
            fn from(value: $type) -> Self {
                Property::$variant(value)
            }
        }

        impl From<Option<$type>> for Property {
            fn from(value: Option<$type>) -> Self {
                value.map(|v| v.into()).unwrap_or(Property::Null)
            }
        }
    };
}

macro_rules! from {
    ($type:ty, $variant:ident) => {
        impl From<$type> for Property {
            fn from(value: $type) -> Self {
                Property::$variant(value.into())
            }
        }

        impl From<Option<$type>> for Property {
            fn from(value: Option<$type>) -> Self {
                value.map(|v| v.into()).unwrap_or(Property::Null)
            }
        }
    };
}

try_from!(u64, Id);
try_from!(i64, Integer);
try_from!(f64, Real);
try_from!(bool, Boolean);
try_from!(String, Text);
try_from!(Vec<u8>, Blob);

from!(i32, Integer);
from!(&str, Text);
from!(&[u8], Blob);

impl<const N: usize> From<[u8; N]> for Property {
    fn from(value: [u8; N]) -> Self {
        Property::Blob(value.into())
    }
}

impl<const N: usize> From<Option<[u8; N]>> for Property {
    fn from(value: Option<[u8; N]>) -> Self {
        value.map(|v| v.into()).unwrap_or(Property::Null)
    }
}

impl<const N: usize> From<&[u8; N]> for Property {
    fn from(value: &[u8; N]) -> Self {
        Property::Blob(value.to_vec())
    }
}

impl<const N: usize> From<Option<&[u8; N]>> for Property {
    fn from(value: Option<&[u8; N]>) -> Self {
        value.map(|v| v.into()).unwrap_or(Property::Null)
    }
}
