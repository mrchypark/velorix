use std::{cmp::Ordering, collections::BTreeMap};
/// Transient, bounded Serde values used only while constructing Arrow columns.
#[derive(Debug)]
pub(super) enum Value {
    Unit,
    Bool(bool),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    F64(f64),
    String(String),
    Seq(Vec<Value>),
    Map(BTreeMap<Value, Value>),
    Bytes(Vec<u8>),
}
impl Value {
    fn tag(&self) -> u8 {
        match self {
            Self::Unit => 0,
            Self::Bool(_) => 1,
            Self::I64(_) => 2,
            Self::U64(_) => 3,
            Self::I128(_) => 4,
            Self::U128(_) => 5,
            Self::F64(_) => 6,
            Self::String(_) => 7,
            Self::Seq(_) => 8,
            Self::Map(_) => 9,
            Self::Bytes(_) => 10,
        }
    }
}
impl Ord for Value {
    fn cmp(&self, b: &Self) -> Ordering {
        let tag = self.tag().cmp(&b.tag());
        if tag != Ordering::Equal {
            return tag;
        }
        match (self, b) {
            (Self::Unit, Self::Unit) => Ordering::Equal,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::I64(a), Self::I64(b)) => a.cmp(b),
            (Self::U64(a), Self::U64(b)) => a.cmp(b),
            (Self::I128(a), Self::I128(b)) => a.cmp(b),
            (Self::U128(a), Self::U128(b)) => a.cmp(b),
            (Self::F64(a), Self::F64(b)) => a.total_cmp(b),
            (Self::String(a), Self::String(b)) => a.cmp(b),
            (Self::Seq(a), Self::Seq(b)) => a.cmp(b),
            (Self::Map(a), Self::Map(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            _ => unreachable!(),
        }
    }
}
impl PartialOrd for Value {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        Some(self.cmp(b))
    }
}
impl PartialEq for Value {
    fn eq(&self, b: &Self) -> bool {
        self.cmp(b) == Ordering::Equal
    }
}
impl Eq for Value {}
