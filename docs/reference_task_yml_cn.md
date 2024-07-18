# 任务描述参考手册

oss_pipe 通过 yml 格式描述需要执行的任务

## yaml描述文件基本结构
```
type: transfer
task_id: '7219591351229353985'
name: transfer_oss2oss
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
  objects_per_batch: 100
  task_parallelism: 11
  max_errors: 1
  meta_dir: /tmp/meta_dir
  target_exists_skip: false
  start_from_checkpoint: false
  large_file_size: 50m
  multi_part_chunk_size: 10m
  multi_part_chunks_per_batch: 10
  multi_part_parallelism: 22
  exclude:
  - test/t3/*
  - test/t4/*
  include:
  - test/t1/*
  - test/t2/*
  transfer_type: stock
  last_modify_filter:
    filter_type: Greater
    timestamp: 1721284711
```
- type: 描述任务类型
- task_id： 、name：  描述任务唯一id以及任务名称
- source: 描述远端存储类别
- target: 描述目标端存储类别
- attributes: 描述任务属性

## yml 参数详解

<table>
    <tr>
	  <td >配置项</td>
	  <td>字段属性</td>
	  <td>必填</td>  
      <td>描述</td>
      <td>示例</td>   
	</tr >
    <tr>
	   <td> type </td>
	   <td>String</td>
       <td>是</td>
       <td>任务类型，transfer 或 compare，详细执行类型通过：oss_pipe  parameters task_type 查看</td>
       <td>type: transfer/compare</td>
	</tr>
     <tr>
	   <td>task_id</td>
	   <td>String</td>
       <td>否</td>
       <td>任务id，为空时由系统生成</td>
       <td>task_id: '7219552894540976129'</td>
	</tr>
    <tr>
	   <td>name</td>
	   <td>String</td>
       <td>否</td>
       <td>任务名称，为空时系统生成默认名称</td>
       <td>name: transfer_oss2oss</td>
	</tr>
    <tr>
	   <td>source</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为本地目录时，指定本地目录</td>
       <td>source: /tmp/source_files</td>
	</tr>
    <tr>
	   <td>source-provider</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，描述对象存储提供商。值为ALI、JD等，支持的对象存储提供商通过oss_pipe parameters provider 查询</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20;provider: JD/JRSS/ALI/AWS/HUAWEI/COS/MINIO</td>
	</tr>
    <tr>
	   <td>source-access_key_id</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 access_key。</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20;access_key_id: xxxx</td>
	</tr>
    <tr>
	   <td>source-secret_access_key</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 secret key</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20; secret_access_key: xxxx</td>
	</tr>
    <tr>
	   <td>source-endpoint</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储endpoint</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20;endpoint: http://oss-cn-beijing.aliyuncs.com</td>
	</tr>
    <tr>
	   <td>source-region</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储region</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20;region: cn-north-1</td>
	</tr>
    <tr>
	   <td>source-bucket</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储bucket</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20;bucket: bucket_name</td>
	</tr>
    <tr>
	   <td>source-prefix</td>
	   <td>String</td>
       <td>否</td>
       <td>当源为对象存储时，指定prefix时，只对该prefix下的对象进行操作</td>
       <td>source:<br>
        &#20;&#20;&#20;&#20;prefix: source/test/prefix/</td>
	</tr>
    <tr>
	   <td>target</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为本地目录时，指定本地目录</td>
       <td>target: /tmp/target_files</td>
	</tr>
    <tr>
	   <td>target-provider</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，描述对象存储提供商。值为ALI、JD等，支持的对象存储提供商通过oss_pipe parameters provider 查询</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20;provider: JD/JRSS/ALI/AWS/HUAWEI/COS/MINIO<</td>
	</tr>
    <tr>
	   <td>target-access_key_id</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，指定对象存储的 access_key。</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20;access_key_id: xxxx</td>
	</tr>
    <tr>
	   <td>target-secret_access_key</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 secret key</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20; secret_access_key: xxxx</td>
	</tr>
    <tr>
	   <td>target-endpoint</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储endpoint</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20;endpoint: http://oss-cn-beijing.aliyuncs.com</td>
	</tr>
    <tr>
	   <td>target-region</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储region</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20;region: cn-north-1</td>
	</tr>
    <tr>
	   <td>target-bucket</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储bucket</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20;bucket: bucket_name</td>
	</tr>
    <tr>
	   <td>target-prefix</td>
	   <td>String</td>
       <td>否</td>
       <td>当目标为对象存储时，指定prefix时，只对该prefix下的对象进行操作</td>
       <td>target:<br>
        &#20;&#20;&#20;&#20;prefix: target/prefix/</td>
	</tr>
    <tr>
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
	</tr>
    <tr>
	   <td>attributes-task_parallelism</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，任务并行度，既同时执行任务批次的数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;task_parallelism: 16</td>
	</tr>
    <tr>
	   <td>attributes-max_errors</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，任务执行期间容忍的最大错误数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20; max_errors: 3</td>
	</tr>
    <tr>
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
	</tr>
    <tr>
	   <td>attributes-meta_dir</td>
	   <td>String</td>
       <td>否</td>
       <td>任务属性，元数据存储位置，默认路径/tmp/meta_dir</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;meta_dir: /root/meta_dir</td>
	</tr>
    <tr>
	   <td>attributes-target_exists_skip</td>
	   <td>bool</td>
       <td>否</td>
       <td>任务属性，当target存在同名对象时不传送对象，默认值为false</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;target_exists_skip: false</td>
	</tr>
    <tr>
	   <td>attributes-start_from_checkpoint</td>
	   <td>bool</td>
       <td>否</td>
       <td>任务属性，是否从checkpoint开始执行任务，用于任务中断后接续执行，默认值false</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;start_from_checkpoint: true</td>
	</tr>
    <tr>
	   <td>attributes-large_file_size</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，超过该参数设置尺寸的文件，文件切片传输</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;large_file_size: 50M</td>
	</tr>
    <tr>
	   <td>attributes-multi_part_chunk_size</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，对象分片尺寸</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;multi_part_chunk_size: 10m</td>
	</tr>
    <tr>
	   <td>attributes-multi_part_chunks_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批执行的分片数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20; multi_part_chunks_per_batch: 20</td>
	</tr>
    <tr>
	   <td>attributes-multi_part_parallelism</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，分片批次执行的并行度</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;multi_part_parallelism: 8</td>
	</tr>
    <tr>
	   <td>attributes-exclude</td>
	   <td>list</td>
       <td>否</td>
       <td>任务属性，配置排除对象的正则表达式列表，符合列表的对象将不被处理</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;oexclude: <br>
        &#20;&#20;&#20;&#20;- test/t3/* <br>
        &#20;&#20;&#20;&#20;- test/t4/*</td>
	</tr>
    <tr>
	   <td>attributes-include</td>
	   <td>list</td>
       <td>是</td>
       <td>任务属性，配置正则表达式列表，程序只处理符合列表的对象</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;include: <br>
        &#20;&#20;&#20;&#20;- test/t3/* <br>
        &#20;&#20;&#20;&#20;- test/t4/*</td></td>
	</tr>
    <tr>
	   <td>attributes-transfer_type</td>
	   <td>String</td>
       <td>是</td>
       <td>任务属性，传输类型 stock/full,目前支持存量和全量模式</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;transfer_type: stock/full</td>
	</tr>
    <tr>
	   <td>attributes-last_modify_filter</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，根据需要筛选符合实际戳条件的对象进行传输</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;last_modify_filter: <br>
        &#20;&#20;&#20;&#20;&#20;&#20;&#20;&#20;filter_type: Greater/Less <br>
        &#20;&#20;&#20;&#20;&#20;&#20;&#20;&#20;timestamp: 1721284711</td>
	</tr>
</table>