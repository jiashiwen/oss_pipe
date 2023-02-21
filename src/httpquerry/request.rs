use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::httpquerry::globalhttpclient::GLOBAL_HTTP_CLIENT;

#[derive(Debug, Deserialize, Serialize)]
pub struct Token {
    pub token: String,
}

#[derive(Deserialize, Debug)]
pub struct QueryResult<T> {
    pub code: i32,
    pub msg: String,
    pub data: Option<T>,
}

pub async fn query_login() -> Result<Option<Token>> {
    let res = GLOBAL_HTTP_CLIENT.post("http://127.0.0.1:3000/login")
        .body(r#"{"username": "root",	"password": "123456"}"#)
        .header("Content-Type", "application/json")
        .send().await?.json::<QueryResult<Token>>().await?;
    Ok(res.data)
}

pub async fn query_baidu() -> Result<String> {
    let url = "https://www.baidu.com";
    let resp = GLOBAL_HTTP_CLIENT.post(url)
        .send().await?;
    let body_bytes = resp.bytes().await?;
    let str = String::from_utf8(body_bytes.to_vec())?;

    Ok(str)
}

#[cfg(test)]
mod test {
    use super::*;

    //cargo test httpquerry::request::test::test_query_baidu -- --nocapture
    #[test]
    fn test_query_baidu() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let res = query_baidu().await;
            println!("res is {:?}", res);
        });
    }

    //cargo test httpquerry::request::test::test_query_login -- --nocapture
    #[test]
    fn test_query_login() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let res = query_login().await;
            println!("res is {:?}", res);
        });
    }
}