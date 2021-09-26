use crate::store::Property;
use std::collections::HashMap;

pub trait Sealed {
    fn build(self) -> HashMap<String, Property>;
}

/// A set of parameters which can be bound when running a
/// [`Statement`][crate::Statement].
///
/// Parameters are a list of key-value pairs. The keys are
/// strings, the values are represented as [`Property`][Property]
/// values.
pub trait Params: Sealed {}

impl Sealed for () {
    fn build(self) -> HashMap<String, Property> {
        HashMap::new()
    }
}

impl Params for () {}

impl<K, V> Sealed for (K, V)
where
    K: AsRef<str>,
    V: Into<Property>,
{
    fn build(self) -> HashMap<String, Property> {
        [self].build()
    }
}

impl<K, V> Params for (K, V)
where
    K: AsRef<str>,
    V: Into<Property>,
{
}

impl<K, V, const N: usize> Sealed for [(K, V); N]
where
    K: AsRef<str>,
    V: Into<Property>,
{
    fn build(self) -> HashMap<String, Property> {
        <Self as IntoIterator>::into_iter(self)
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect()
    }
}

impl<K, V, const N: usize> Params for [(K, V); N]
where
    K: AsRef<str>,
    V: Into<Property>,
{
}

impl<K, V, const N: usize> Sealed for &[(K, V); N]
where
    K: AsRef<str>,
    V: Clone + Into<Property>,
{
    fn build(self) -> HashMap<String, Property> {
        self.iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.clone().into()))
            .collect()
    }
}

impl<K, V, const N: usize> Params for &[(K, V); N]
where
    K: AsRef<str>,
    V: Clone + Into<Property>,
{
}

impl<K, V> Sealed for &[(K, V)]
where
    K: AsRef<str>,
    V: Clone + Into<Property>,
{
    fn build(self) -> HashMap<String, Property> {
        self.iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.clone().into()))
            .collect()
    }
}

impl<K, V> Params for &[(K, V)]
where
    K: AsRef<str>,
    V: Clone + Into<Property>,
{
}

impl<K, V> Sealed for Vec<(K, V)>
where
    K: AsRef<str>,
    V: Into<Property>,
{
    fn build(self) -> HashMap<String, Property> {
        self.into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect()
    }
}

impl<K, V> Params for Vec<(K, V)>
where
    K: AsRef<str>,
    V: Into<Property>,
{
}

impl<K, V> Sealed for HashMap<K, V>
where
    K: AsRef<str>,
    V: Into<Property>,
{
    fn build(self) -> HashMap<String, Property> {
        self.into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect()
    }
}

impl<K, V> Params for HashMap<K, V>
where
    K: AsRef<str>,
    V: Into<Property>,
{
}

macro_rules! impl_tuple {
    ($($idx:tt, $key:ident, $val:ident),+) => {
        impl<$($key, $val,)+> Sealed for ($( ($key, $val), )+)
        where
            $($key: AsRef<str>,)+
            $($val: Into<Property>,)+
        {
            fn build(self) -> HashMap<String, Property> {
                let mut map = HashMap::new();
                $( map.insert(self.$idx.0.as_ref().to_string(), self.$idx.1.into()); )+
                map
            }
        }

        impl<$($key, $val,)+> Params for ($( ($key, $val), )+)
        where
            $($key: AsRef<str>,)+
            $($val: Into<Property>,)+
        {
        }
    };
}

impl_tuple!(0, K0, V0);
impl_tuple!(0, K0, V0, 1, K1, V1);
impl_tuple!(0, K0, V0, 1, K1, V1, 2, K2, V2);
impl_tuple!(0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3);
impl_tuple!(0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4);
impl_tuple!(0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5);
impl_tuple!(0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6);
impl_tuple!(0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9, 10, K10, V10
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9, 10, K10, V10, 11, K11, V11
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9, 10, K10, V10, 11, K11, V11, 12, K12, V12
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9, 10, K10, V10, 11, K11, V11, 12, K12, V12, 13, K13, V13
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9, 10, K10, V10, 11, K11, V11, 12, K12, V12, 13, K13, V13, 14, K14, V14
);
impl_tuple!(
    0, K0, V0, 1, K1, V1, 2, K2, V2, 3, K3, V3, 4, K4, V4, 5, K5, V5, 6, K6, V6, 7, K7, V7, 8, K8,
    V8, 9, K9, V9, 10, K10, V10, 11, K11, V11, 12, K12, V12, 13, K13, V13, 14, K14, V14, 15, K15,
    V15
);
