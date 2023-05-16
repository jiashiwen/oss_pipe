# 参考手册

## 任务类型

* download

  ```yml

  # 任务id，非必填
  task_id: '7064075547986497537'
  # 任务名称，非必填
  name: download task
  # 任务描述，支持的任务可以通过 oss_pipe parameters task_type 查找
  task_desc: !Download
    # 源对象存储
    source:
      # 对象存储供应商，支持的供应商可以通过 oss_pipe  parameters provider 查找
      provider: JD
      # 对象存储 access_key
      access_key_id: access_key_id
      # 对象存储 secret_access
      secret_access_key: secret_access_key
      # 对象存储endpoint，请参考供应商相关文档
      endpoint: http://s3.cn-north-1.jdcloud-oss.com
      # 对象存储区域，请参考供应商相关文档
      region: cn-north-1
      # 对象存储 bucket
      bucket: bucket_name
      # 对象存储prefix
      prefix: test/samples/
    # 下载目标目录
    local_path: /tmp
    # 批次大小，既每批下载文件数据量
    bach_size: 100
    # 并发数量
    task_threads: 12
    # 任务允许的最大错误数，达到最大错误数量则任务停止
    max_errors: 1
    # 任务元数据存储位置
    meta_dir: /tmp/meta_dir
    # 当目标文件已存在时是否下载
    target_exists_skip: false
    # 从checkpoint开始执行
    start_from_checkpoint: false
    # 大文件定义，当文件超过该值时分段下载，单位为Bytes
    large_file_size: 104857600
    # 大文件拆分时每块大小，单位为Bytes
    multi_part_chunk: 10485760
  ```

* transfer

  ```yml
  # 任务id，非必填
  task_id: '7064079488245698561'
  # 任务名称，非必填  
  name: transfer task
  # 任务描述，支持的任务可以通过 oss_pipe parameters task_type 查找  
  task_desc: !Transfer
    # 源对象存储
    source:
      # 对象存储供应商，支持的供应商可以通过 oss_pipe  parameters provider 查找
      provider: Ali
      # 对象存储 access_key
      access_key_id: access_key_id
      # 对象存储 secret_access
      secret_access_key: secret_access_key
      # 对象存储endpoint，请参考供应商相关文档
      endpoint: http://oss-cn-beijing.aliyuncs.com
      # 对象存储区域，请参考供应商相关文档      
      region: cn-north-1
      # 对象存储 bucket
      bucket: bucket_name
      # 对象存储prefix
      prefix: test/samples/
    # 目标对象存储
    target:
      # 对象存储供应商，支持的供应商可以通过 oss_pipe  parameters provider 查找
      provider: JD
      # 对象存储 access_key
      access_key_id: access_key_id
      # 对象存储 secret_access
      secret_access_key: secret_access_key
      # 对象存储endpoint，请参考供应商相关文档
      endpoint: http://s3.cn-north-1.jdcloud-oss.com
      # 对象存储区域，请参考供应商相关文档      
      region: cn-north-1
      # 对象存储 bucket
      bucket: bucket_name
      # 对象存储prefix
      prefix: test/samples/
    # 批次大小，既每批下载文件数据量
    bach_size: 100
    # 并发数量
    task_threads: 12
    # 任务允许的最大错误数，达到最大错误数量则任务停止
    max_errors: 1
    # 任务元数据存储位置
    meta_dir: /tmp/meta_dir
    # 当目标文件已存在时是否下载
    target_exists_skip: false
    # 从checkpoint开始执行
    start_from_checkpoint: false
    # 大文件定义，当文件超过该值时分段下载，单位为Bytes
    large_file_size: 104857600
    # 大文件拆分时每块大小，单位为Bytes
    multi_part_chunk: 10485760
  ```

* upload
  
  ```yml
  # 任务id，非必填
  task_id: '7064079488245698561'
  # 任务名称，非必填  
  name: upload task
  # 任务描述，支持的任务可以通过 oss_pipe parameters task_type 查找  
  task_desc: !Upload
    # 上传目标目录
    local_path: /tmp
    # 目标对象存储
    target:
      # 对象存储供应商，支持的供应商可以通过 oss_pipe  parameters provider 查找
      provider: JD
      # 对象存储 access_key
      access_key_id: access_key_id
      # 对象存储 secret_access
      secret_access_key: secret_access_key
      # 对象存储endpoint，请参考供应商相关文档
      endpoint: http://s3.cn-north-1.jdcloud-oss.com
      # 对象存储区域，请参考供应商相关文档  
      region: cn-north-1
      # 对象存储 bucket
      bucket: bucket_name
      # 对象存储prefix
      prefix: test/samples/
    # 批次大小，既每批上传文件数据量
    bach_size: 100
    # 并发数量
    task_threads: 12
    # 任务允许的最大错误数，达到最大错误数量则任务停止
    max_errors: 1
    # 任务元数据存储位置
    meta_dir: /tmp/meta_dir
    # 当目标文件已存在时是否下载
    target_exists_skip: false
    # 从checkpoint开始执行
    start_from_checkpoint: false
    # 大文件定义，当文件超过该值时分段下载，单位为Bytes
    large_file_size: 104857600
    # 大文件拆分时每块大小，单位为Bytes
    multi_part_chunk: 10485760
  ```

* localtolocal
  
  ```yml
  # 任务id，非必填
  task_id: '7064087275835101185'
  # 任务名称，非必填  
  name: local to local task
  # 任务描述，支持的任务可以通过 oss_pipe parameters task_type 查找  
  task_desc: !LocalToLocal
    # 源目录
    source_path: /tmp/source
    # 目标目录
    target_path: /tmp/target
    # 批次大小，既每批复制文件数据量
    bach_size: 100
    # 并发数量
    task_threads: 12
    # 任务允许的最大错误数，达到最大错误数量则任务停止
    max_errors: 1
    # 任务元数据存储位置
    meta_dir: /tmp/meta_dir
    # 当目标文件已存在时是否下载
    target_exists_skip: false
    # 从checkpoint开始执行
    start_from_checkpoint: false
    # 大文件定义，当文件超过该值时分段下载，单位为Bytes
    large_file_size: 104857600
    # 大文件拆分时每块大小，单位为Bytes
    multi_part_chunk: 104857
  ```

* truncate_bucket  
  
  ```yml
  # 任务id，非必填
  task_id: '7064088180835880961'
  # 任务名称，非必填  
  name: truncate bucket task
  # 任务描述，支持的任务可以通过 oss_pipe parameters task_type 查找 
  task_desc: !TruncateBucket
    # 要清理的bucket描述
    oss:
      # 对象存储供应商，支持的供应商可以通过 oss_pipe  parameters provider 查找
      provider: JD
      # 对象存储 access_key
      access_key_id: access_key_id
      # 对象存储 secret_access
      secret_access_key: secret_access_key
      # 对象存储endpoint，请参考供应商相关文档
      endpoint: http://s3.cn-north-1.jdcloud-oss.com
      # 对象存储区域，请参考供应商相关文档  
      region: cn-north-1
      # 对象存储 bucket
      bucket: bucket_name
      # 对象存储prefix
      prefix: test/samples/
    # 批次大小，既每批删除文件数据量
    bach_size: 100
    # 并发数量
    task_threads: 12
    # 任务允许的最大错误数，达到最大错误数量则任务停止
    max_errors: 1
    # 任务元数据存储位置
    meta_dir: /tmp/meta_dir
  ```
  
* oss_compare
  
  ```yml
  # 任务id，非必填
  task_id: '7064090414587973633'
  # 任务名称，非必填  
  name: oss compare task
  # 任务描述，支持的任务可以通过 oss_pipe parameters task_type 查找 
  task_desc: !OssCompare
    # 源对象存储
    source:
      provider: Ali
      access_key_id: access_key_id
      secret_access_key: secret_access_key
      endpoint: http://oss-cn-beijing.aliyuncs.com
      region: cn-north-1
      bucket: bucket_name
      prefix: test/samples/
    # 目标对象存储  
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
    exprirs_diff_scope: 10
    start_from_checkpoint: false
  ```
  


## 对象存储提供者