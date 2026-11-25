//! Reject duplicate fields before serde_json::Value can silently discard them.
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Json;
        impl<'de> Visitor<'de> for Json {
            type Value = Unique;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("JSON without duplicate fields")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Unique, E> {
                Ok(Unique(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Unique, E> {
                Ok(Unique(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Unique, E> {
                Ok(Unique(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Unique, E> {
                Number::from_f64(value)
                    .map(|value| Unique(Value::Number(value)))
                    .ok_or_else(|| E::custom("JSON number must be finite"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Unique, E> {
                Ok(Unique(value.into()))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Unique, E> {
                Ok(Unique(value.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Unique, A::Error> {
                let mut values = Vec::new();
                while let Some(Unique(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(Unique(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Unique, A::Error> {
                let mut values = Map::new();
                while let Some((key, Unique(value))) = map.next_entry::<String, Unique>()? {
                    if values.insert(key, value).is_some() {
                        return Err(de::Error::custom("Duplicate external speaker JSON field"));
                    }
                }
                Ok(Unique(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(Json)
    }
}
pub(super) fn parse(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    serde_json::from_slice::<Unique>(bytes).map(|value| value.0)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_duplicate_identity_and_key_fields_without_echoing_secrets() {
        for body in [
            r#"{"speakers":{"bot":{"action":"disable"},"bot":{"action":"disable"}}}"#,
            r#"{"speakers":{"bot":{"action":"set","key":{"kind":"custom","value":"private","value":"second"}}}}"#,
            r#"{"expected_revision":0,"expected_revision":1}"#,
        ] {
            let error = parse(body.as_bytes()).unwrap_err().to_string();
            assert!(error.contains("Duplicate external speaker JSON field"));
            assert!(!error.contains("private"));
        }
        assert_eq!(
            parse(br#"{"speakers":{},"expected_revision":0}"#).unwrap()["expected_revision"],
            0
        );
    }
}
