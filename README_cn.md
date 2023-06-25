# OSS PIPE

OSS PIPE 是一个用于 S3 兼容对象存储间进行文件迁移的工具。

## 主要功能

* Transfer
  对象存储间文件迁移，比如 AWS 到 阿里云的迁移，或从阿里云到华为云的迁移
* Download
  从对象存储下载到本地
* Upload
  本地目录上传到OSS
* LocalToLocal
  本地多线程复制
* TruncateBucket
  清空Bucket （慎用！）
* OssCompare
  object 比较

## 如何使用

参考 [quick_start](docs/quick_start.md)