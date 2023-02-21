use anyhow::Result;
use serde::de;
use serde_json::from_str;

// pub fn json_file_to_struct<'a, T>(filepath: &str) -> Result<T>
//     where
//         T: de::Deserialize<'a>, {
pub fn json_to_struct<'a, T>(content: &'a str) -> Result<T>
where
    T: de::Deserialize<'a>,
{
    let res = from_str::<T>(content).map_err(|e| anyhow::Error::new(e))?;
    Ok(res)
}
