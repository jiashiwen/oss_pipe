use anyhow::Result;
use http::Request as httpreq;
use jdcloud_signer::{Credential, Signer};
use reqwest::Response;

use crate::httpquerry::globalhttpclient::GLOBAL_HTTP_CLIENT;
use crate::httpquerry::module::{InstanceId, JdcloudRequest};

pub fn jdcloud_vm() -> httpreq<String> {
    let _jdreq = JdcloudRequest::<InstanceId> {
        ak: "".to_string(),
        sk: "".to_string(),
        service_name: "".to_string(),
        region: "".to_string(),
        parameters: None,
    };
    let ak = "4107B314B15BCE99A1C781DFCF119F59";
    let sk = "8877CD432EB5738EFF0FA01F630201C9";
    let credential = Credential::new(ak, sk);
    let signer = Signer::new(credential, "vm".to_string(), "cn-north-1".to_string());

    let req = httpreq::builder();
    let mut req = req
        .method("GET")
        .uri("https://vm.jdcloud-api.com/v1/regions/cn-north-1/instances")
        .body("".to_string())
        .unwrap();
    signer.sign_request(&mut req).unwrap();
    req

    // println!("{:?}", req);
}

pub async fn jdcloud_vm_describe_instance() -> Result<Response> {
    let ak = "4107B314B15BCE99A1C781DFCF119F59";
    let sk = "8877CD432EB5738EFF0FA01F630201C9";
    let credential = Credential::new(ak, sk);
    let signer = Signer::new(credential, "vm".to_string(), "cn-north-1".to_string());

    let httpreq = httpreq::builder();
    let mut httpreq = httpreq
        .method("GET")
        .uri("https://vm.jdcloud-api.com/v1/regions/cn-north-1/instances")
        .body("".to_string())
        .unwrap();
    signer.sign_request(&mut httpreq)?;
    let header = httpreq.headers();

    let resp = GLOBAL_HTTP_CLIENT
        .get("https://vm.jdcloud-api.com/v1/regions/cn-north-1/instances")
        .headers(header.clone())
        .send()
        .await?;
    Ok(resp)
}

#[cfg(test)]
mod test {
    use super::*;

    //cargo test httpquerry::jdcloud::test::test_jdcloud_vm -- --nocapture
    #[test]
    fn test_jdcloud_vm() {
        let req = jdcloud_vm();
        println!("{:?}", req);
    }

    //cargo test httpquerry::jdcloud::test::test_jdcloud_vm_describe_instance -- --nocapture
    #[test]
    fn test_jdcloud_vm_describe_instance() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let resp = jdcloud_vm_describe_instance().await;
            println!("{:?}", resp.unwrap().text().await);
        });
    }
}
