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
- [x] jrss适配，相关文档<http://jrss-portal-public.jdfmgt.com/>
- [x] 多线程验证rayon 和 tokio两个方案
- [x] 编写多线程程序
- [x] checkpoint 设计与实现 transfer
- [x] checkpoint 设计与实现 download
- [x] checkpoint 设计与实现 upload
- [x] 最大错误数机制，到达最大错误数，任务停止
- [ ] 错误数据持久化与解析，配合checkpoint实现断点续传
- [ ] 实现获取object属性函数
- [x] 支持文件过期时间
- [x] 大文件支持<https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html>
  - [x] 大文件分片上传
  - [x] 大文件分片下载
- [ ] 多任务模式，统一描述文件支持多任务同事运行
- [x] 增加local_to_local 任务，多线程复制目录文件
  - [x] 大文件文件流切分
  - [x] 大文件流写入
- [x] 当源端文件不存在则视作成功处理，记录offset
- [x] 支持oss文件过期时间
- [ ] 静态校验功能
  - [x] target 是否存在
  - [x] 过期时间差值是否在一个范围之内
  - [x] content_length 源于目标是否一致
  - [ ] 精确校验模式：（待验证）可否通过流方式读取源与目标文件，校验buffer是否一致，如此只消耗网络流量
- [ ] 支持oss存储类型，通过 set_storage_class 实现
- [ ] 增加模板输出功能，增加文件名参数。
- [ ] 新增precheck，并给出校验清单
  - [ ] 归档文件不能访问需提前校验
- [ ] 支持last_modify_grater ,最后更改时间源端大于上次同步时获取object list 列表的初始时间戳,配置增量参数incremental，当该参数为真，在每次同步结束时获取本次同步起始时间戳，遍历源，生成时间戳大于该时间戳的的对象列表，执行同步任务，重复以上过程，直到object list内容为空
- [ ] 编写makefile 实现交叉编译
- [x] 进度展示
  - [x] 命令行进度选型
  - [x] 迁移总文件数统计
  - [x] 迁移完成文件数统计
  - [x] 计算总体进度百分比
  - [ ] checkpoint新增total_lines 和 finished_lines
- [ ] 源端文件分析，分析大小文件所占比例，便于制定并发数量策略
- [x] upload 及 local2local 任务，通过使用inotify记录增量
- [x] 由于loop占用线程时间太长消耗系统资源，需要改进文件怎量同步部分代码，使用定时器定期轮询文件
  - [ ] 定义增量记录文件，记录每次轮询的文件名及offset
  - [ ] 利用 tokio::sleep 减轻cpu负载
  - [ ] 定时器定时按记录文件的offset执行增量任务
- [x] 文件上传断点续传实现
  - [ ] 实现机制：先开启notify记录变动文件；每次断点遍历上传文件目录，取last_modify 大于  checkpoint 记录时间戳的文件上传。需要考虑大量文件的遍历时间，需要做相关实验验证
  - [x] 兼容minio
    - [x] minio 环境部署
    - [x] 适配minio
  - [ ] 设计多任务snapshot，统一管理任务状态
    - [ ] 任务相关状态字段定义，任务checkpoint，任务错误数，任务offset map，停止标识
  - [ ] 为便于扩展，新增objectstorage enum，用来区分不同数据源
  - [x] 解决modify create modify重复问题
  - [x] 增量逻辑新增正则过滤
  - [ ] 新增指定时间戳同步
  - [x] 将oss 增量代码归并至oss client
  - [ ] 适配七牛
  - [x] 支持从指定时间戳同步
  - [ ] prefix map,将源prefix 映射到目标的另一个prefx
  - [ ] 全量、存量、增量分处理
  - [ ] 修改源为oss的同步机制，base taget 计算removed 和 modified objects

## 校验项

- [x]last_modify 时间戳，目标大于源

## 错误处理及断点续传机制

- 日志及执行offset记录
  - 执行任务时每线程offset日志后写，记录完成的记录在文件中的offset，offset文件名为 文件名前缀+第一条记录的offset
  - 某一记录执行出错时记录record及offset，当错误数达到某一阀值，停止任务
- 断点续传机制
  - 先根据错误记录日志做补偿同步，检查源端对象是否存在，若不存在则跳过
  - 通过offset日志找到每个文件中的最大值，并所有文件的最大值取最小值，再与checkpoint中的offset比较取最小值，作为objectlist的起始offset。

## 增量实现

### 源为oss时的机制

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

### 源为本地文件系统（linux）

通过 inotify 获取目录变更事件（需要验证 <https://crates.io/crates/notify）>

## 面临问题 -- sdk 兼容性问题，需要实验

- [x] 如何获取object属性
- [x] 如何判断object是否存在
- [x] 文件追加和覆盖哪个效率更高待验证

## 测试验证

- [x] tokio  实现多线程稳定性，通过循环多线程upload测试,异步情况需重新考虑trait。
- [ ] 验证 从oss流式读取行，内存直接put 到oss

## 多线程任务

- [x] 多线程任务模型设计

## 服务化改造

- [ ] 服务化架构设计
- [ ] 任务meta data设计

## 分布式改造

git log --since=2023-11-06 --until=2023-11-10 --pretty=tformat: --numstat | awk '{ add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\n", add, subs, loc }'


## 记事本

11月9
改进增量断点续传方案，抓取target 的 remove 和  modify，执行完成后，执行增量逻辑
