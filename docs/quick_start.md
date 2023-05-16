# quick start

## How to build

* 安装rust编译环境

```rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* 构建

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


### 执行任务