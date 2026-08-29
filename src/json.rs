use serde_json::Value as JsonValue;
use crate::schema::Value;
use crate::error::SkvsError;

/// Extract a value from a JSON string using a JSON path
pub fn json_extract(json_str: &str, path: &str) -> Result<Value, SkvsError> {
    let val: JsonValue = serde_json::from_str(json_str)
        .map_err(|e| SkvsError::Json(format!("Invalid JSON: {}", e)))?;

    // Paths are conventionally given SQLite-style, e.g. "$.name" or "$".
    // Strip the leading "$" (and a following ".") so plain dotted paths work too.
    let path = path.strip_prefix('$').unwrap_or(path);
    let path = path.strip_prefix('.').unwrap_or(path);

    if path.is_empty() {
        return Ok(json_value_to_skvs(&val));
    }

    let parts: Vec<&str> = path.split('.').collect();
    let mut current = &val;
    for part in parts {
        if part.starts_with('[') && part.ends_with(']') {
            let idx: usize = part[1..part.len()-1].parse()
                .map_err(|_| SkvsError::Json("Invalid array index".into()))?;
            if let Some(arr) = current.as_array() {
                if idx < arr.len() {
                    current = &arr[idx];
                } else {
                    return Ok(Value::Null);
                }
            } else {
                return Ok(Value::Null);
            }
        } else {
            if let Some(obj) = current.as_object() {
                if let Some(v) = obj.get(part) {
                    current = v;
                } else {
                    return Ok(Value::Null);
                }
            } else {
                return Ok(Value::Null);
            }
        }
    }
    Ok(json_value_to_skvs(current))
}

/// Create a JSON array
pub fn json_array(values: &[Value]) -> Result<String, SkvsError> {
    let json_items: Vec<JsonValue> = values.iter()
        .map(|v| skvs_value_to_json(v))
        .collect();
    Ok(serde_json::to_string(&json_items)?)
}

/// Create a JSON object
pub fn json_object(keys: &[String], values: &[Value]) -> Result<String, SkvsError> {
    if keys.len() != values.len() {
        return Err(SkvsError::Json("Keys and values length mismatch".into()));
    }
    let mut obj = serde_json::Map::new();
    for (k, v) in keys.iter().zip(values.iter()) {
        obj.insert(k.clone(), skvs_value_to_json(v));
    }
    Ok(serde_json::to_string(&obj)?)
}

/// Set a value in JSON at a path
pub fn json_set(json_str: &str, path: &str, new_value: &Value) -> Result<String, SkvsError> {
    let mut val: JsonValue = serde_json::from_str(json_str)
        .map_err(|e| SkvsError::Json(format!("Invalid JSON: {}", e)))?;
    
    let parts: Vec<&str> = path.split('.').collect();
    let last = parts.last().ok_or_else(|| SkvsError::Json("Empty path".into()))?;
    let parent_parts = &parts[..parts.len()-1];
    let mut current = &mut val;
    for part in parent_parts {
        if part.starts_with('[') && part.ends_with(']') {
            let idx: usize = part[1..part.len()-1].parse()
                .map_err(|_| SkvsError::Json("Invalid array index".into()))?;
            if let Some(arr) = current.as_array_mut() {
                if idx < arr.len() {
                    current = &mut arr[idx];
                } else {
                    return Err(SkvsError::Json("Array index out of bounds".into()));
                }
            } else {
                return Err(SkvsError::Json("Path not an array".into()));
            }
        } else {
            if let Some(obj) = current.as_object_mut() {
                if !obj.contains_key(*part) {
                    obj.insert(part.to_string(), JsonValue::Object(serde_json::Map::new()));
                }
                current = obj.get_mut(*part).unwrap();
            } else {
                return Err(SkvsError::Json("Path not an object".into()));
            }
        }
    }
    if last.starts_with('[') && last.ends_with(']') {
        let idx: usize = last[1..last.len()-1].parse()
            .map_err(|_| SkvsError::Json("Invalid array index".into()))?;
        if let Some(arr) = current.as_array_mut() {
            if idx < arr.len() {
                arr[idx] = skvs_value_to_json(new_value);
            } else {
                return Err(SkvsError::Json("Array index out of bounds".into()));
            }
        } else {
            return Err(SkvsError::Json("Target is not an array".into()));
        }
    } else {
        if let Some(obj) = current.as_object_mut() {
            obj.insert(last.to_string(), skvs_value_to_json(new_value));
        } else {
            return Err(SkvsError::Json("Target is not an object".into()));
        }
    }
    Ok(serde_json::to_string(&val)?)
}

fn json_value_to_skvs(val: &JsonValue) -> Value {
    match val {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() { Value::Integer(i) }
            else if let Some(f) = n.as_f64() { Value::Real(f) }
            else { Value::Null }
        }
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(arr) => Value::Blob(serde_json::to_string(arr).unwrap_or_default().into_bytes()),
        JsonValue::Object(obj) => Value::Blob(serde_json::to_string(obj).unwrap_or_default().into_bytes()),
    }
}

fn skvs_value_to_json(val: &Value) -> JsonValue {
    match val {
        Value::Null => JsonValue::Null,
        Value::Integer(i) => JsonValue::Number((*i).into()),
        Value::Real(f) => {
            if let Some(n) = serde_json::Number::from_f64(*f) {
                JsonValue::Number(n)
            } else {
                JsonValue::Null
            }
        }
        Value::Text(s) => JsonValue::String(s.clone()),
        Value::Blob(b) => {
            if let Ok(json) = serde_json::from_slice(b) {
                json
            } else {
                JsonValue::String(base64::encode(b))
            }
        }
    }
}