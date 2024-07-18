# 任务描述参考手册

oss_pipe 通过 yml 格式描述需要执行的任务

## yml 示例


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
       <td>type: transfer</td>
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
        &#20;&#20;&#20;&#20;provider: ALI</td>
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
        &#20;&#20;&#20;&#20;provider: ALI</td>
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
	   <td>String</td>
       <td>fou</td>
       <t，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
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
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
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
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
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
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
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
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
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
	   <td>attributes-objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &#20;&#20;&#20;&#20;objects_per_batch: 100</td>
	</tr>
</table>