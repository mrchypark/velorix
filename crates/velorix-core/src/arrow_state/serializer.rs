use super::*;
use serde::ser::{self, SerializeMap, SerializeSeq};
impl ser::Error for StateError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        err(message)
    }
}
pub(super) struct Budget {
    pub size: usize,
    pub limit: usize,
}
impl Budget {
    fn charge(&mut self, extra: usize) -> Result<(), StateError> {
        self.size = self
            .size
            .checked_add(48)
            .and_then(|n| n.checked_add(extra))
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| err("runtime_checkpoint_bytes"))?;
        Ok(())
    }
}
pub(super) struct Serializer<'a> {
    pub budget: &'a mut Budget,
    pub depth: usize,
}
impl<'a> Serializer<'a> {
    fn leaf(self, v: Value, extra: usize) -> Result<Value, StateError> {
        if self.depth > MAX_DEPTH {
            return Err(err("depth limit"));
        }
        self.budget.charge(extra)?;
        Ok(v)
    }
    fn compound(self, map: bool) -> Result<Compound<'a>, StateError> {
        if self.depth >= MAX_DEPTH {
            return Err(err("depth limit"));
        }
        self.budget.charge(0)?;
        Ok(Compound {
            budget: self.budget,
            depth: self.depth + 1,
            values: Vec::new(),
            map: map.then(BTreeMap::new),
            key: None,
            variant: None,
        })
    }
}
macro_rules! scalar {
    ($method:ident,$ty:ty,$variant:ident) => {
        fn $method(self, v: $ty) -> Result<Value, StateError> {
            self.leaf(Value::$variant(v.into()), 0)
        }
    };
}
impl<'a> serde::Serializer for Serializer<'a> {
    type Ok = Value;
    type Error = StateError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;
    scalar!(serialize_bool, bool, Bool);
    scalar!(serialize_i8, i8, I64);
    scalar!(serialize_i16, i16, I64);
    scalar!(serialize_i32, i32, I64);
    scalar!(serialize_i64, i64, I64);
    scalar!(serialize_u8, u8, U64);
    scalar!(serialize_u16, u16, U64);
    scalar!(serialize_u32, u32, U64);
    scalar!(serialize_u64, u64, U64);
    fn serialize_f32(self, value: f32) -> Result<Value, StateError> {
        self.serialize_f64(f64::from(value))
    }
    fn serialize_i128(self, v: i128) -> Result<Value, StateError> {
        if let Ok(v) = i64::try_from(v) {
            return self.serialize_i64(v);
        }
        self.leaf(Value::I128(v), 16)
    }
    fn serialize_u128(self, v: u128) -> Result<Value, StateError> {
        if let Ok(v) = u64::try_from(v) {
            return self.serialize_u64(v);
        }
        self.leaf(Value::U128(v), 16)
    }
    fn serialize_f64(self, v: f64) -> Result<Value, StateError> {
        if !v.is_finite() {
            return Err(err("nonfinite number"));
        }
        self.leaf(Value::F64(v), 0)
    }
    fn serialize_char(self, v: char) -> Result<Value, StateError> {
        let mut b = [0; 4];
        self.serialize_str(v.encode_utf8(&mut b))
    }
    fn serialize_str(self, v: &str) -> Result<Value, StateError> {
        if self.depth > MAX_DEPTH {
            return Err(err("depth limit"));
        }
        self.budget.charge(v.len())?;
        Ok(Value::String(v.into()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Value, StateError> {
        if self.depth > MAX_DEPTH {
            return Err(err("depth limit"));
        }
        self.budget.charge(v.len())?;
        Ok(Value::Bytes(v.into()))
    }
    fn serialize_none(self) -> Result<Value, StateError> {
        self.serialize_unit()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<Value, StateError> {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<Value, StateError> {
        self.leaf(Value::Unit, 0)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Value, StateError> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        v: &'static str,
    ) -> Result<Value, StateError> {
        self.serialize_str(v)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<Value, StateError> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        v: &T,
    ) -> Result<Value, StateError> {
        let mut c = self.compound(true)?;
        c.serialize_entry(variant, v)?;
        SerializeMap::end(c)
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Compound<'a>, StateError> {
        self.compound(false)
    }
    fn serialize_tuple(self, _: usize) -> Result<Compound<'a>, StateError> {
        self.compound(false)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, StateError> {
        self.compound(false)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, StateError> {
        let mut outer = self.compound(true)?;
        outer.map = None;
        outer.budget.charge(variant.len())?;
        outer.budget.charge(0)?;
        outer.variant = Some(variant);
        outer.depth += 1;
        Ok(outer)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Compound<'a>, StateError> {
        self.compound(true)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, StateError> {
        self.compound(true)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, StateError> {
        let mut c = self.compound(true)?;
        c.budget.charge(variant.len())?;
        c.budget.charge(0)?;
        c.variant = Some(variant);
        c.depth += 1;
        Ok(c)
    }
}
pub(super) struct Compound<'a> {
    budget: &'a mut Budget,
    depth: usize,
    values: Vec<Value>,
    map: Option<BTreeMap<Value, Value>>,
    key: Option<Value>,
    variant: Option<&'static str>,
}
impl Compound<'_> {
    fn value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<Value, StateError> {
        v.serialize(Serializer {
            budget: self.budget,
            depth: self.depth,
        })
    }
    fn finish(self) -> Result<Value, StateError> {
        if self.key.is_some() {
            return Err(err("map value missing"));
        }
        let value = if let Some(map) = self.map {
            if self.variant.is_some() && !self.values.is_empty() {
                Value::Seq(self.values)
            } else {
                Value::Map(map)
            }
        } else {
            Value::Seq(self.values)
        };
        Ok(if let Some(v) = self.variant {
            Value::Map([(Value::String(v.into()), value)].into())
        } else {
            value
        })
    }
}
impl SerializeSeq for Compound<'_> {
    type Ok = Value;
    type Error = StateError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), StateError> {
        let v = self.value(v)?;
        self.values.push(v);
        Ok(())
    }
    fn end(self) -> Result<Value, StateError> {
        self.finish()
    }
}
macro_rules! seq {
    ($trait:ident,$method:ident) => {
        impl ser::$trait for Compound<'_> {
            type Ok = Value;
            type Error = StateError;
            fn $method<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), StateError> {
                SerializeSeq::serialize_element(self, v)
            }
            fn end(self) -> Result<Value, StateError> {
                self.finish()
            }
        }
    };
}
seq!(SerializeTuple, serialize_element);
seq!(SerializeTupleStruct, serialize_field);
seq!(SerializeTupleVariant, serialize_field);
impl SerializeMap for Compound<'_> {
    type Ok = Value;
    type Error = StateError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), StateError> {
        if self.key.is_some() {
            return Err(err("duplicate pending key"));
        }
        self.key = Some(self.value(v)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), StateError> {
        let key = self.key.take().ok_or_else(|| err("map key missing"))?;
        let value = self.value(v)?;
        if self
            .map
            .as_mut()
            .ok_or_else(|| err("map container"))?
            .insert(key, value)
            .is_some()
        {
            return Err(err("duplicate map key"));
        }
        Ok(())
    }
    fn end(self) -> Result<Value, StateError> {
        self.finish()
    }
}
macro_rules! object {
    ($trait:ident) => {
        impl ser::$trait for Compound<'_> {
            type Ok = Value;
            type Error = StateError;
            fn serialize_field<T: Serialize + ?Sized>(
                &mut self,
                k: &'static str,
                v: &T,
            ) -> Result<(), StateError> {
                self.serialize_entry(k, v)
            }
            fn end(self) -> Result<Value, StateError> {
                self.finish()
            }
        }
    };
}
object!(SerializeStruct);
object!(SerializeStructVariant);
