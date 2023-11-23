# Getting Stated

## How to build

* 安装rust编译环境

```rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* 构建

```shell
apt update
apt install openssl
apt install libssl1.1
apt install libssl-dev
apt install -y pkg-config
```

```shell
git clone https://github.com/jiashiwen/oss_pipe.git
cd oss_pipe
cargo build --release
```

## 基本使用

oss_pipe 支持命令执行模式和交互式执行模式。
您可以通过 oss_pipe [subcommand] 执行执行任务，比如

```shell
oss_pipe parameters provider
```

也可以通过 oss_pipe -i 命里进入交互模式。交互模式支持按 'tab' 键进行子命令提示。

### 定义任务

通过 oss_pipe template 命令生成模板

```shell
oss_pipe template transfer oss2oss /tmp/transfer.yml
```

transfer.yml 文件内容

```yml
task_id: '7132608357105537025'
name: transfer oss to oss
task_desc:
  type: transfer
  source:
    provider: ALI
    access_key_id: access_key_id
    secret_access_key: secret_access_key
    endpoint: http://oss-cn-beijing.aliyuncs.com
    region: cn-north-1
    bucket: bucket_name
    prefix: test/samples/
  target:
    provider: JD
    access_key_id: access_key_id
    secret_access_key: secret_access_key
    endpoint: http://s3.cn-north-1.jdcloud-oss.com
    region: cn-north-1
    bucket: bucket_name
    prefix: test/samples/
  attributes:
    bach_size: 100
    task_threads: 12
    max_errors: 1
    meta_dir: /tmp/meta_dir
    target_exists_skip: false
    start_from_checkpoint: false
    large_file_size: 104857600
    multi_part_chunk: 10485760
    exclude:
    - test/t3/*
    - test/t4/*
    include:
    - test/t1/*
    - test/t2/*
    continuous: false
    transfer_type: Stock
```

修改 access_key_id secret_access_key 等参数，适配自己的任务。template 命令按照任务类型创建模版,模板描述请参考[参考手册](reference_cn.md)。parameters 支持参数查询，包括支持的provider 以及 任务类型

### 执行任务

osstask 子命令用于执行任务

```shell
oss_pipe task exec filepath/task.yml
```

## 同步任务流程

![同步任务流程](./images/同步流程图-v3.png)
