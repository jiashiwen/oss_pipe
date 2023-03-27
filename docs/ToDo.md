# ToDo

## 技术验证

- [ ] 验证tokio multy threads 在执行时是否阻塞

## 基本功能

- [x] 工程整理，升级依赖，剔除不必要的依赖和代码
- [x] 实现基础的jd s3 client
- [x] 实现基础的 阿里云 s3 client
- [x] 实现 aws client
- [x] 抽象各个厂商的oss client 形成统一的产生函数
- [x] 抽象ossaction trait
- [x] 实现读取和上传object bytes
- [x] jrss适配，相关文档http://jrss-portal-public.jdfmgt.com/
- [ ] checkpoint 设计与实现
- [ ] 错误数据持久化与解析，配合checkpoint实现断点续传
- [ ] 大文件支持https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html


## 多线程任务

- [ ] 多线程任务模型设计


## 服务化改造

- [ ] 服务化架构设计

## 分布式改造

- [ ]
