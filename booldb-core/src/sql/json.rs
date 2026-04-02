use crate::error::{BoolDBError, Result};
use crate::types::Value;

/// Extract a value from a JSON string using a JSONPath expression.
///
/// Supported path syntax:
/// - `$.field` — top-level field
/// - `$.field.subfield` — nested field access
/// - `$.field[0]` — array index access
/// - `$.field[0].subfield` — combined
///
/// Returns `Value::Null` if the path does not exist.
pub fn json_extract(json_str: &str, path: &str) -> Result<Value> {
    let parsed: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| BoolDBError::Sql(format!("Invalid JSON: {}", e)))?;

    let segments = parse_path(path)?;
    let result = navigate(&parsed, &segments);

    Ok(json_value_to_value(result))
}

/// Validate that a string is valid JSON.
pub fn validate_json(s: &str) -> Result<()> {
    let _: serde_json::Value =
        serde_json::from_str(s).map_err(|e| BoolDBError::Sql(format!("Invalid JSON: {}", e)))?;
    Ok(())
}

/// A segment in a JSON path.
#[derive(Debug, Clone)]
enum PathSegment {
    Field(String),
    Index(usize),
}

/// Parse a JSONPath like `$.name.address[0].city` into segments.
fn parse_path(path: &str) -> Result<Vec<PathSegment>> {
    let path = path.trim();
    if !path.starts_with('$') {
        return Err(BoolDBError::Sql(format!(
            "JSON path must start with '$', got: {}",
            path
        )));
    }

    let rest = if path.starts_with("$.") {
        &path[2..]
    } else if path == "$" {
        return Ok(Vec::new());
    } else {
        &path[1..]
    };

    if rest.is_empty() {
        return Ok(Vec::new());
    }

    let mut segments = Vec::new();
    let mut current = String::new();

    let chars: Vec<char> = rest.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '.' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Field(current.clone()));
                    current.clear();
                }
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Field(current.clone()));
                    current.clear();
                }
                i += 1;
                let mut idx_str = String::new();
                while i < chars.len() && chars[i] != ']' {
                    idx_str.push(chars[i]);
                    i += 1;
                }
                let idx: usize = idx_str.parse().map_err(|_| {
                    BoolDBError::Sql(format!("Invalid array index: {}", idx_str))
                })?;
                segments.push(PathSegment::Index(idx));
            }
            c => {
                current.push(c);
            }
        }
        i += 1;
    }

    if !current.is_empty() {
        segments.push(PathSegment::Field(current));
    }

    Ok(segments)
}

/// Navigate a serde_json::Value using path segments.
fn navigate<'a>(value: &'a serde_json::Value, segments: &[PathSegment]) -> &'a serde_json::Value {
    let mut current = value;
    for seg in segments {
        match seg {
            PathSegment::Field(name) => {
                current = current.get(name).unwrap_or(&serde_json::Value::Null);
            }
            PathSegment::Index(idx) => {
                current = current.get(*idx).unwrap_or(&serde_json::Value::Null);
            }
        }
    }
    current
}

/// Convert a serde_json::Value to our Value type.
fn json_value_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        // Objects and arrays are returned as their JSON string representation.
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_top_level() {
        let json = r#"{"name": "Alice", "age": 30, "active": true}"#;
        assert_eq!(json_extract(json, "$.name").unwrap(), Value::Text("Alice".into()));
        assert_eq!(json_extract(json, "$.age").unwrap(), Value::Integer(30));
        assert_eq!(json_extract(json, "$.active").unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_extract_nested() {
        let json = r#"{"address": {"city": "NYC", "zip": "10001"}}"#;
        assert_eq!(
            json_extract(json, "$.address.city").unwrap(),
            Value::Text("NYC".into())
        );
    }

    #[test]
    fn test_extract_array() {
        let json = r#"{"tags": ["admin", "dev", "ops"]}"#;
        assert_eq!(
            json_extract(json, "$.tags[0]").unwrap(),
            Value::Text("admin".into())
        );
        assert_eq!(
            json_extract(json, "$.tags[2]").unwrap(),
            Value::Text("ops".into())
        );
    }

    #[test]
    fn test_extract_nested_array() {
        let json = r#"{"users": [{"name": "Alice"}, {"name": "Bob"}]}"#;
        assert_eq!(
            json_extract(json, "$.users[1].name").unwrap(),
            Value::Text("Bob".into())
        );
    }

    #[test]
    fn test_extract_missing_path() {
        let json = r#"{"name": "Alice"}"#;
        assert_eq!(json_extract(json, "$.missing").unwrap(), Value::Null);
        assert_eq!(json_extract(json, "$.a.b.c").unwrap(), Value::Null);
    }

    #[test]
    fn test_extract_null() {
        let json = r#"{"value": null}"#;
        assert_eq!(json_extract(json, "$.value").unwrap(), Value::Null);
    }

    #[test]
    fn test_extract_float() {
        let json = r#"{"price": 9.99}"#;
        assert_eq!(json_extract(json, "$.price").unwrap(), Value::Float(9.99));
    }

    #[test]
    fn test_extract_object_as_text() {
        let json = r#"{"nested": {"a": 1}}"#;
        let result = json_extract(json, "$.nested").unwrap();
        match result {
            Value::Text(s) => assert!(s.contains("\"a\"") && s.contains("1")),
            _ => panic!("Expected Text for nested object"),
        }
    }

    #[test]
    fn test_invalid_json() {
        assert!(json_extract("not json", "$.x").is_err());
    }

    #[test]
    fn test_validate_json() {
        assert!(validate_json(r#"{"a": 1}"#).is_ok());
        assert!(validate_json("not json").is_err());
    }
}
