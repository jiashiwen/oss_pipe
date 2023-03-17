use anyhow::Result;
use serde::de;
use serde_json::from_str;

#[allow(dead_code)]
pub fn json_to_struct<'a, T>(content: &'a str) -> Result<T>
where
    T: de::Deserialize<'a>,
{
    let res = from_str::<T>(content).map_err(|e| anyhow::Error::new(e))?;
    Ok(res)
}
