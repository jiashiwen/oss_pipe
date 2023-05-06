# ToDo

## 技术验证

- [x] 验证tokio multy threads 在执行时是否阻塞

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
- [x] checkpoint 设计与实现 transfer
- [x] checkpoint 设计与实现 download
- [ ] checkpoint 设计与实现 upload
- [x] 最大错误数机制，到达最大错误数，任务停止
- [ ] 错误数据持久化与解析，配合checkpoint实现断点续传
- [ ] 实现获取object属性函数
- [ ] 支持过期时间
- [ ] 支持文件过期时间
- [x] 大文件支持https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html
  - [x] 大文件分片上传
  - [ ] 大文件分片下载
- [ ] 多任务模式，统一描述文件支持多任务同事运行 
- [x] 增加local_to_local 任务，多线程复制目录文件
  - [ ] 大文件文件流切分
  - [ ] 大文件流写入
- [ ] 当源端文件不存在则视作成功处理，记录offset
- [ ] 支持oss文件过期时间
- [ ] 静态校验功能
  - [ ] target 是否存在
  - [ ] 过期时间差值是否在一个范围之内
  - [ ] content_length 源于目标是否一致
- [ ] 支持oss存储类型，通过 set_storage_class 实现
- [ ] 增加模板输出功能，增加文件名参数。
- [ ] 新增precheck，并给出校验清单
  - [ ] 归档文件不能访问需提前校验

## 校验项

- last_modify 时间戳，目标大于源

## 错误处理及断点续传机制

* 日志及执行offset记录
  * 执行任务时每线程offset日志后写，记录完成的记录在文件中的offset，offset文件名为 文件名前缀+第一条记录的offset
  * 某一记录执行出错时记录record及offset，当错误数达到某一阀值，停止任务
* 断点续传机制
  * 先根据错误记录日志做补偿同步，检查源端对象是否存在，若不存在则跳过
  * 通过offset日志找到每个文件中的最大值，并所有文件的最大值取最小值，再与checkpoint中的offset比较取最小值，作为objectlist的起始offset。

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

## 面临问题 -- sdk 兼容性问题，需要实验

- [x] 如何获取object属性
- [x] 如何判断object是否存在
- [x] 文件追加和覆盖哪个效率更高待验证

## 测试验证

- [ ] tokio  实现多线程稳定性，通过循环多线程upload测试,异步情况需重新考虑trait。

## 多线程任务

- [ ] 多线程任务模型设计

## 服务化改造

- [ ] 服务化架构设计

## 分布式改造

- [ ]


ai 大赛，只能导购助理，通过对话方式筛选商品