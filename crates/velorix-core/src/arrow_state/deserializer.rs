use super::*;
use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
impl de::Error for StateError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        err(message)
    }
}
pub(super) struct Deserializer<'a> {
    pub state: &'a Inner,
    pub index: usize,
    pub key: bool,
}
impl Deserializer<'_> {
    fn kind(&self) -> u8 {
        self.state
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(self.index)
    }
    fn text(&self) -> &str {
        self.state
            .batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(self.index)
    }
}
macro_rules! map_number {
    ($method:ident, $ty:ty, $visit:ident) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, StateError> {
            if self.key && self.kind() == 5 {
                visitor.$visit(self.text().parse::<$ty>().map_err(err)?)
            } else {
                self.deserialize_any(visitor)
            }
        }
    };
}
impl<'de> de::Deserializer<'de> for Deserializer<'de> {
    type Error = StateError;
    map_number!(deserialize_i8, i8, visit_i8);
    map_number!(deserialize_i16, i16, visit_i16);
    map_number!(deserialize_i32, i32, visit_i32);
    map_number!(deserialize_i64, i64, visit_i64);
    map_number!(deserialize_i128, i128, visit_i128);
    map_number!(deserialize_u8, u8, visit_u8);
    map_number!(deserialize_u16, u16, visit_u16);
    map_number!(deserialize_u32, u32, visit_u32);
    map_number!(deserialize_u64, u64, visit_u64);
    map_number!(deserialize_u128, u128, visit_u128);
    fn deserialize_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        let b = &self.state.batch;
        let i = self.index;
        match self.kind() {
            0 => v.visit_unit(),
            1 => v.visit_bool(
                b.column(2)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(i),
            ),
            2 => v.visit_i64(
                b.column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i),
            ),
            3 => v.visit_u64(
                b.column(4)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(i),
            ),
            4 => v.visit_f64(
                b.column(5)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i),
            ),
            5 => v.visit_borrowed_str(
                b.column(6)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i),
            ),
            6 => v.visit_seq(Access {
                state: self.state,
                index: i + 1,
                end: self.state.ends[i],
                pending: false,
            }),
            7 => v.visit_map(Access {
                state: self.state,
                index: i + 1,
                end: self.state.ends[i],
                pending: false,
            }),
            8 => v.visit_seq(de::value::SeqDeserializer::<_, StateError>::new(
                b.column(7)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(i)
                    .iter()
                    .copied(),
            )),
            9 => v.visit_i128(i128::from_le_bytes(
                b.column(7)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(i)
                    .try_into()
                    .map_err(err)?,
            )),
            10 => v.visit_u128(u128::from_le_bytes(
                b.column(7)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(i)
                    .try_into()
                    .map_err(err)?,
            )),
            _ => Err(err("node kind")),
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        if self.kind() == 0 {
            v.visit_none()
        } else {
            v.visit_some(self)
        }
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        v: V,
    ) -> Result<V::Value, StateError> {
        v.visit_newtype_struct(self)
    }
    fn deserialize_string<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        if self.key {
            let b = &self.state.batch;
            let i = self.index;
            match self.kind() {
                2 => {
                    return v.visit_string(
                        b.column(3)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i)
                            .to_string(),
                    )
                }
                3 => {
                    return v.visit_string(
                        b.column(4)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(i)
                            .to_string(),
                    )
                }
                _ => {}
            }
        }
        self.deserialize_any(v)
    }
    fn deserialize_str<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        self.deserialize_string(v)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        if self.kind() == 8 {
            v.visit_borrowed_bytes(
                self.state
                    .batch
                    .column(7)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(self.index),
            )
        } else {
            self.deserialize_any(v)
        }
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        self.deserialize_bytes(v)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, StateError> {
        if self.kind() == 5 {
            return v.visit_enum(self.text().to_owned().into_deserializer());
        }
        let count = self
            .state
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(self.index);
        if self.kind() != 7 || count != 2 {
            return Err(err("enum representation"));
        }
        v.visit_enum(Enum {
            state: self.state,
            key: self.index + 1,
        })
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, StateError> {
        v.visit_unit()
    }
    serde::forward_to_deserialize_any! {bool f32 f64 char unit unit_struct seq tuple tuple_struct map struct identifier}
}
struct Access<'a> {
    state: &'a Inner,
    index: usize,
    end: usize,
    pending: bool,
}
impl<'de> SeqAccess<'de> for Access<'de> {
    type Error = StateError;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, StateError> {
        if self.index >= self.end {
            return Ok(None);
        }
        let i = self.index;
        self.index = self.state.ends[i];
        seed.deserialize(Deserializer {
            state: self.state,
            index: i,
            key: false,
        })
        .map(Some)
    }
}
impl<'de> MapAccess<'de> for Access<'de> {
    type Error = StateError;
    fn next_key_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, StateError> {
        if self.pending {
            return Err(err("map value missing"));
        }
        if self.index >= self.end {
            return Ok(None);
        }
        let i = self.index;
        self.index = self.state.ends[i];
        self.pending = true;
        seed.deserialize(Deserializer {
            state: self.state,
            index: i,
            key: true,
        })
        .map(Some)
    }
    fn next_value_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<T::Value, StateError> {
        if !self.pending || self.index >= self.end {
            return Err(err("map key missing"));
        }
        let i = self.index;
        self.index = self.state.ends[i];
        self.pending = false;
        seed.deserialize(Deserializer {
            state: self.state,
            index: i,
            key: false,
        })
    }
}
struct Enum<'a> {
    state: &'a Inner,
    key: usize,
}
impl<'de> de::EnumAccess<'de> for Enum<'de> {
    type Error = StateError;
    type Variant = Self;
    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self), StateError> {
        let name = seed.deserialize(Deserializer {
            state: self.state,
            index: self.key,
            key: false,
        })?;
        Ok((name, self))
    }
}
impl<'de> de::VariantAccess<'de> for Enum<'de> {
    type Error = StateError;
    fn unit_variant(self) -> Result<(), StateError> {
        Deserialize::deserialize(Deserializer {
            state: self.state,
            index: self.state.ends[self.key],
            key: false,
        })
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, StateError> {
        seed.deserialize(Deserializer {
            state: self.state,
            index: self.state.ends[self.key],
            key: false,
        })
    }
    fn tuple_variant<V: Visitor<'de>>(self, _: usize, v: V) -> Result<V::Value, StateError> {
        de::Deserializer::deserialize_seq(
            Deserializer {
                state: self.state,
                index: self.state.ends[self.key],
                key: false,
            },
            v,
        )
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, StateError> {
        de::Deserializer::deserialize_map(
            Deserializer {
                state: self.state,
                index: self.state.ends[self.key],
                key: false,
            },
            v,
        )
    }
}
