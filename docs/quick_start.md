# quick start

## How to build

* 安装rust编译环境

```rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* 构建

```shell
apt install -y pkg-config
```

```shell
git clone https://github.com/jiashiwen/oss_pipe.git
cd oss_pipe
cargo build --release
```

## 基本使用

### 定义任务

通过 oss_pipe template 命令生成模板

```shell
oss_pipe template transfer /tmp/transfer.yml
```

transfer.yml 文件内容

```yml
task_id: '7071651847576096769'
name: transfer task
task_desc: !Transfer
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
  bach_size: 100
  task_threads: 12
  max_errors: 1
  meta_dir: /tmp/meta_dir
  target_exists_skip: false
  start_from_checkpoint: false
  large_file_size: 104857600
  multi_part_chunk: 10485760
  exclude: null
  include: null
```

修改 access_key_id secret_access_key 等参数，适配自己的任务。template 命令按照任务类型创建模版，支持的任务类型请参考参考手册。parameters 支持参数查询，包括支持的provider 以及 任务类型

### 执行任务