# s3proxy

## 本地存储，无校验
s3proxy.conf
```
s3proxy.ignore-unknown-headers=true
s3proxy.authorization=none
#s3proxy.cors-allow-all=true
#s3proxy.authorization=aws-v2-or-v4
#s3proxy.identity=abc
#s3proxy.credential=abcd
s3proxy.endpoint=http://0.0.0.0:8080
jclouds.provider=filesystem
jclouds.filesystem.basedir=/root/s3proxy_store
```
target/s3proxy --properties s3proxy.conf
创建bucket
```
curl --request PUT http://localhost:8080/testbucket
```

使用 oss_pipe 上传

## 本地存储，有校验
s3proxy.conf
```
s3proxy.ignore-unknown-headers=true
#s3proxy.authorization=none
s3proxy.cors-allow-all=true
s3proxy.authorization=aws-v2-or-v4
s3proxy.identity=abc
s3proxy.credential=abcd
s3proxy.endpoint=http://0.0.0.0:8080
jclouds.provider=filesystem
jclouds.filesystem.basedir=/root/s3proxy_store
```
target/s3proxy --properties s3proxy.conf

安装 mc
```
go install github.com/minio/mc@latest
```

mc alias set s3proxy http://127.0.0.1:8080 abc abcd

mc ls s3proxy

## azure Blob
azure.conf

```
s3proxy.ignore-unknown-headers=true
s3proxy.cors-allow-all=true
s3proxy.authorization=aws-v2-or-v4
s3proxy.identity=abc
s3proxy.credential=abcd
s3proxy.endpoint=http://0.0.0.0:8080
#jclouds.azureblob.auth=azureKey
jclouds.provider=azureblob
jclouds.identity=storeaccount
jclouds.credential=xxxxxxxxxxxxxxxxxx
jclouds.endpoint=https://account.blob.core.windows.net
```

mc
az storage blob list --account-key xxxxxxxxx --account-name xxxx --container-name xxxx

上传下载通过，与s3交互和大文件分片待测试