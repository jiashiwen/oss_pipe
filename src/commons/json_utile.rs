use std::fs;

use anyhow::Result;
use serde::{de, Deserialize};
use serde_json::from_str;

#[allow(dead_code)]
pub fn json_to_struct<'a, T>(content: &'a str) -> Result<T>
where
    T: de::Deserialize<'a>,
{
    let res = from_str::<T>(content).map_err(|e| anyhow::Error::new(e))?;
    Ok(res)
}

pub fn struct_to_json_string<T>(value: &T) -> Result<String>
where
    T: ?Sized + serde::Serialize,
{
    let json = serde_json::to_string(value)?;
    Ok(json)
}

pub fn read_json_file<T>(path: &str) -> Result<T>
where
    T: for<'a> Deserialize<'a>,
{
    let contents = fs::read_to_string(path)?;
    let r = from_str::<T>(contents.as_str())?;
    Ok(r)
}
