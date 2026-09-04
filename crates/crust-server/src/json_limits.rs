//! Bounded JSON parsing and retention for untrusted protocol payloads.

use std::collections::HashSet;
use std::fmt;
use std::io;

use crust::media::{JsonObject, LoadOutcome, MediaTrack};
use crust::resources::ResourceLimits;
use serde::de::{DeserializeOwned, DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JsonPolicy {
    max_retained_bytes: usize,
    max_depth: usize,
    max_elements: usize,
}

impl From<&ResourceLimits> for JsonPolicy {
    fn from(limits: &ResourceLimits) -> Self {
        Self {
            max_retained_bytes: limits.max_retained_json_bytes.get(),
            max_depth: limits.max_json_depth.get(),
            max_elements: limits.max_json_elements.get(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct JsonLimitError;

impl fmt::Display for JsonLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON exceeds the configured structural or retention limit")
    }
}

impl std::error::Error for JsonLimitError {}

pub(crate) fn parse_bounded_json<T: DeserializeOwned>(
    bytes: &[u8],
    policy: JsonPolicy,
) -> Result<T, JsonLimitError> {
    let mut budget = ParseBudget {
        elements: 0,
        policy,
    };
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = ValueSeed {
        budget: &mut budget,
        depth: 1,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| JsonLimitError)?;
    deserializer.end().map_err(|_| JsonLimitError)?;
    serde_json::from_value(value).map_err(|_| JsonLimitError)
}

pub(crate) fn validate_json_object(
    object: &JsonObject,
    policy: JsonPolicy,
) -> Result<(), JsonLimitError> {
    let mut elements = 1;
    for value in object.values() {
        validate_value(value, 2, &mut elements, policy)?;
    }
    let mut writer = LimitedWriter::new(policy.max_retained_bytes);
    serde_json::to_writer(&mut writer, object).map_err(|_| JsonLimitError)?;
    Ok(())
}

pub(crate) fn validate_media_track(
    track: &MediaTrack,
    policy: JsonPolicy,
) -> Result<(), JsonLimitError> {
    validate_json_object(&track.plugin_info, policy)
}

pub(crate) fn validate_load_outcome(
    outcome: &LoadOutcome,
    policy: JsonPolicy,
) -> Result<(), JsonLimitError> {
    match outcome {
        LoadOutcome::Track(track) => validate_media_track(track, policy),
        LoadOutcome::Search(tracks) => tracks
            .iter()
            .try_for_each(|track| validate_media_track(track, policy)),
        LoadOutcome::Playlist {
            plugin_info,
            tracks,
            ..
        } => {
            validate_json_object(plugin_info, policy)?;
            tracks
                .iter()
                .try_for_each(|track| validate_media_track(track, policy))
        }
        LoadOutcome::NoMatches => Ok(()),
    }
}

struct ParseBudget {
    elements: usize,
    policy: JsonPolicy,
}

impl ParseBudget {
    fn admit<E: serde::de::Error>(&mut self, depth: usize) -> Result<(), E> {
        if depth > self.policy.max_depth {
            return Err(E::custom("JSON nesting depth exceeds configured limit"));
        }
        self.elements = self
            .elements
            .checked_add(1)
            .ok_or_else(|| E::custom("JSON element count overflow"))?;
        if self.elements > self.policy.max_elements {
            return Err(E::custom("JSON element count exceeds configured limit"));
        }
        Ok(())
    }
}

struct ValueSeed<'a> {
    budget: &'a mut ParseBudget,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.budget.admit::<D::Error>(self.depth)?;
        deserializer.deserialize_any(ValueVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}

struct ValueVisitor<'a> {
    budget: &'a mut ParseBudget,
    depth: usize,
}

impl<'de> Visitor<'de> for ValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        ValueSeed {
            budget: self.budget,
            depth: self.depth,
        }
        .deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(ValueSeed {
            budget: self.budget,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(A::Error::custom("duplicate JSON object key"));
            }
            let value = map.next_value_seed(ValueSeed {
                budget: self.budget,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

fn validate_value(
    value: &Value,
    depth: usize,
    elements: &mut usize,
    policy: JsonPolicy,
) -> Result<(), JsonLimitError> {
    if depth > policy.max_depth {
        return Err(JsonLimitError);
    }
    *elements = elements.checked_add(1).ok_or(JsonLimitError)?;
    if *elements > policy.max_elements {
        return Err(JsonLimitError);
    }
    match value {
        Value::Array(values) => values
            .iter()
            .try_for_each(|value| validate_value(value, depth + 1, elements, policy)),
        Value::Object(values) => values
            .values()
            .try_for_each(|value| validate_value(value, depth + 1, elements, policy)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Ok(()),
    }
}

struct LimitedWriter {
    written: usize,
    maximum: usize,
}

impl LimitedWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            written: 0,
            maximum,
        }
    }
}

impl io::Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("JSON byte count overflow"))?;
        if next > self.maximum {
            return Err(io::Error::other(
                "retained JSON exceeds configured byte limit",
            ));
        }
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> JsonPolicy {
        JsonPolicy {
            max_retained_bytes: 24,
            max_depth: 3,
            max_elements: 5,
        }
    }

    #[test]
    fn parsing_rejects_duplicate_depth_and_element_ambiguity() {
        assert!(parse_bounded_json::<Value>(br#"{"a":1,"a":2}"#, policy()).is_err());
        assert!(parse_bounded_json::<Value>(br#"[[[0]]]"#, policy()).is_err());
        assert!(parse_bounded_json::<Value>(br#"[0,1,2,3,4]"#, policy()).is_err());
        assert_eq!(
            parse_bounded_json::<Value>(br#"{"a":[1,true]}"#, policy()).unwrap(),
            serde_json::json!({"a":[1,true]})
        );
    }

    #[test]
    fn retained_object_is_checked_without_allocating_a_serialized_copy() {
        let accepted = serde_json::from_value(serde_json::json!({"x": [1]})).unwrap();
        assert!(validate_json_object(&accepted, policy()).is_ok());
        let rejected =
            serde_json::from_value(serde_json::json!({"x": "012345678901234567890123"})).unwrap();
        assert!(validate_json_object(&rejected, policy()).is_err());
    }
}
