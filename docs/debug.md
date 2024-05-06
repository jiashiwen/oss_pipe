# Debug

## multi part upload cpu 高

```
 let mut part_number = 0;
    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let file_len = file.metadata().unwrap().len();
    let mut readed = 0;


    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().unwrap();
    println!("upload_id is {}", upload_id);

    loop {
        let mut buf = vec![0; chunk_size];
        let read_count = file.read(&mut buf).unwrap();
        readed += read_count;
        println!("readed/total:{}/{}", readed, file_len);
        part_number += 1;

        if read_count == 0 {
            break;
        }

        let body = &buf[..read_count];
        let stream = ByteStream::from(body.to_vec());
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;

        let completed_part = CompletedPart::builder()
            .e_tag(upload_part_res.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build();

        upload_parts.push(completed_part);

        if read_count != chunk_size {
            break;
        }
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
```

aws_sigv4::http_request::canonical_request::CanonicalRequest::payload_hash 占用70%
aws rust sdk runtime_plugins 字段为私有，不可变更，提issue


flamegraph -o flamegraph_00.svg --pid 707018

修改建议
为 UploadPartInputBuilder、PutObject、GetObjectInputBuilder 新增 set_payload_override， 以便使用aws_sigv4::http_request::SignableBody改变对象传递时频繁调用sigv4造成cpu使用率过高的问题

      let payload_override = aws_sigv4::http_request::SignableBody::UnsignedPayload;
      let upload_part_res = client
            .upload_part()
            .set_payload_override(payload_override)
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(stream)
            .part_number(p.part_num)
            .send()
            .await?;

impl UploadPartInputBuilder {
     pub fn set_payload_override(mut self, payload_override: ::aws_sigv4::http_request::SignableBody) -> Self {
        self.inner.signing_options.payload_override = payload_override;;
        self
    }
}

payload_override: aws_sigv4::http_request::SignableBody::UnsignedPayload,


Added set_payload_override to UploadPartInputBuilder, PutObject, and GetObjectInputBuilder to prevent frequent calls to sigv4 when using aws_sigv4::http_request::SignableBody to change object transfers, causing high CPU usage.


