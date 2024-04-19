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


flamegraph -o flamegraph_00.svg --pid  10073

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

impl UploadPartFluentBuilder {
    pub async fn send(
        self,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        ::aws_smithy_runtime_api::client::result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(::aws_smithy_runtime_api::client::result::SdkError::construction_failure)?;
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle.runtime_plugins.clone().with_client_plugin(
                crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
                    crate::presigning::PresigningConfig::expires_in(
                        std::time::Duration::from_secs(3000),
                    )
                    .unwrap(),
                    ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
                ),
            ),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }

    pub async fn send_with_plugins(
        self,
        presigning_config: PresigningConfig,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(result::SdkError::construction_failure)?;

        let plugin = crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
            presigning_config,
            ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
        );
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle
                .runtime_plugins
                .clone()
                .with_client_plugin(plugin),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }

}



我已经通过修改 sdk/s3/src/operation/upload_part/builders.rs 假如新函数 send_with_plugins 实现了这个功能。
修改代码如下：

```
use ::aws_smithy_runtime_api::client::result;
use crate::presigning::PresigningConfig;

impl UploadPartFluentBuilder {
    pub async fn send(
        self,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        ::aws_smithy_runtime_api::client::result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(::aws_smithy_runtime_api::client::result::SdkError::construction_failure)?;
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle.runtime_plugins.clone().with_client_plugin(
                crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
                    crate::presigning::PresigningConfig::expires_in(
                        std::time::Duration::from_secs(3000),
                    )
                    .unwrap(),
                    ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
                ),
            ),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }


    /// Sends the request and returns the response with clinet plugin.
    ///
    /// If an error occurs, an `SdkError` will be returned with additional details that
    /// can be matched against.
    pub async fn send_with_plugins(
        self,
        presigning_config: PresigningConfig,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(result::SdkError::construction_failure)?;

        let plugin = crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
            presigning_config,
            ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
        );
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle
                .runtime_plugins
                .clone()
                .with_client_plugin(plugin),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }

}
```

使用用例

```
let presigning = PresigningConfig::expires_in(std::time::Duration::from_secs(3000))?;
        let upload_part_res = self
            .client
            .upload_part()
            .upload_id(upload_id)
            .part_number(part_number)
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send_with_plugins(presigning)
            .await?;
```

flamegraph -o flamegraph_01.svg --pid 2017221


Duration { seconds: 2524, nanoseconds: 680871254 }
