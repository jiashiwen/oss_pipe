#[cfg(test)]
mod test {

    //cargo test checkpoint::fileposition::test::test_fileposition -- --nocapture
    #[test]
    fn test_fileposition() {
        println!("fileposition");
        // let rt = tokio::runtime::Runtime::new().unwrap();
        // let oss_jd = get_jd_oss_description();
        // let jd = oss_jd.gen_oss_client_ref();

        // rt.block_on(async {
        //     let client = jd.unwrap();
        //     let r = client
        //         .append_object_list_to_file(
        //             "jsw-bucket".to_string(),
        //             None,
        //             5,
        //             None,
        //             "/tmp/jd_obj_list".to_string(),
        //         )
        //         .await;

        //     if let Err(e) = r {
        //         println!("{}", e.to_string());
        //         return;
        //     }
        // });
    }
}
