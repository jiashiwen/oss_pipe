// use std::collections::HashMap;

// use oss_rust_sdk::async_object::*;
// use oss_rust_sdk::oss::OSS;
// use tokio::runtime::Runtime;

use aliyun_oss_client::{file::File, BucketName, Client, Query, QueryKey};
use futures::{pin_mut, StreamExt};
use http::response;
use tokio::runtime::Runtime;

// // pub struct AliOssClient {
// //     pub ali_oss_client: OSS,
// // }

// fn async_get_object_demo() {

//     let oss_instance = OSS::new(
//         "LTAI5t8QwychjAoTTx3P",
//         "JhXIzDXRRnHZK0aqVSZesFYto",
//         "oss-cn-beijing.aliyuncs.com",
//         "ali-oss-jsw",
//     );

//     let rt = Runtime::new().expect("failed to start runtime");

//     rt.block_on(async move {
//         let r = oss_instance
//             .get_object("test/Cargo.toml", None::<HashMap<&str, &str>>, None)
//             .await
//             .unwrap();
//         println!("buffer = {:?}", String::from_utf8(r.to_vec()));
//     });
// }

fn async_get_object_demo() {
    let bucket = BucketName::new("ali-oss-jsw").unwrap();
    let oss_instance = Client::new(
        "LTAI5t8QwychjXPe1AoTP".into(),
        "JhXIzDXRRnHoTKeEZK0esFYto".into(),
        "oss-cn-beijing.aliyuncs.com".into(),
        bucket,
    );

    let rt = Runtime::new().expect("failed to start runtime");

    rt.block_on(async move {
        let r = oss_instance
            .get_object("test/Cargo.toml", ..)
            .await
            .unwrap();
        println!("{:?}", String::from_utf8(r).unwrap());
    });
}

fn get_obj_list_demo() {
    let bucket = BucketName::new("ali-oss-jsw").unwrap();
    let client = Client::new(
        "LTAI5t8QwychjXPe1AoTTx3P".into(),
        "JhXIzDXRRnHoTKeEZK0aqVSZesFYto".into(),
        "oss-cn-beijing.aliyuncs.com".into(),
        bucket,
    );

    let rt = Runtime::new().expect("failed to start runtime");
    rt.block_on(async move {
        let mut query = Query::new();
        query.insert("max-keys", 3u16);
        // query.insert(QueryKey::Prefix, "test/");
        // let object_list = client.get_object_list(query).await.unwrap();

        // let object_list = client
        //     .get_bucket_info()
        //     .await
        //     .unwrap()
        //     .get_object_list(query)
        //     .await;

        // for item in object_list.into_iter() {
        //     println!("{:?}", item);
        // }
        let response = client
            .get_object_list([(QueryKey::MaxKeys, 5u8.into())])
            .await
            .unwrap();
        for item in response.object_iter() {
            println!("objects list: {:?}", item.path_string());
        }
        // println!("objects list: {:?}", response);
        // println!(
        //     "list: {:?}, token: {:?}",
        //     object_list,
        //     object_list.next_continuation_token()
        // );
        // let stream = object_list.into_stream();

        // pin_mut!(stream);

        // let second_list = stream.next().await;
        // let third_list = stream.next().await;

        // println!("second_list: {:?}", second_list);
        // println!("third_list: {:?}", third_list);
    });
}

#[cfg(test)]
mod test {
    use super::{async_get_object_demo, get_obj_list_demo};

    //cargo test s3::ali_oss::test::test_async_get_object_demo -- --nocapture
    #[test]
    fn test_async_get_object_demo() {
        async_get_object_demo();
    }

    //cargo test s3::ali_oss::test::test_get_obj_list_demo -- --nocapture
    #[test]
    fn test_get_obj_list_demo() {
        get_obj_list_demo();
    }
}
