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
- [x] 多线程验证rayon 和 tokio两个方案
- [x] 编写多线程程序
- [ ] checkpoint 设计与实现 transfer
- [ ] checkpoint 设计与实现 download
- [ ] checkpoint 设计与实现 upload
- [ ] 最大错误数机制，到达最大错误数，任务停止
- [ ] 错误数据持久化与解析，配合checkpoint实现断点续传
- [ ] 实现获取object属性函数
- [ ] 大文件支持https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html

## 校验项

- last_modify 时间戳，目标大于源

## 增量实现

- 回源实现方式
  - 完成一次全量同步
  - 配置对象存储回源
  - 应用切换到目标对象存储
  - 再次发起同步，只同步目标不存在或时间戳大于目标对象的对象

- 逼近法实现近似增量
  - 应用程序设置为非删除操作
  - 完成一次全量同步
  - 二次同步，只同步新增和修改(时间戳大于目标的)，并记录更新数量
  - 知道新增更新在一个可接接受的范围，切换应用
  - 再次同步，只同步新增和修改(时间戳大于目标的)

# 面临问题 -- sdk 兼容性问题，需要实验

- [ ] 如何获取object属性
- [ ] 如何判断object是否存在

## 测试验证

- [ ] tokio  实现多线程稳定性，通过循环多线程upload测试,异步情况需重新考虑trait。

## 多线程任务

- [ ] 多线程任务模型设计

## 服务化改造

- [ ] 服务化架构设计

## 分布式改造

- [ ]
