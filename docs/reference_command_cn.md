# 命令参考手册

oss_pipe 同时支持命令行模式和交互模式 oss_pipe -i 进入交互模式。交互模式使用'tab'键进行子命令提示。
可以执行 tree 命令列出命令及子命令的层次关系
```
oss_pipe
├── task
│   ├── exec
│   └── analyze
├── template
│   ├── transfer
│   │   ├── oss2oss
│   │   ├── oss2local
│   │   ├── local2oss
│   │   └── local2local│   
│   └── compare
├── parameters
│   ├── provider
│   └── task_type
├── gen_file
├── gen_files
├── exit
└── tree
```

## 命令详解

### task
task 子命令用于执行任务相关操作
<table>
	<tr>
	    <th colspan="4">task</th>
	</tr >
    <tr>
	  <td >命令</td>
	  <td>描述  </td>
	  <td>参数</td>  
      <td>示例</td>  
	</tr >
    <tr>
	   <td>exec</td>
	   <td>按任务描述文件执行任务</td>
       <td>&lt;taskfile&gt;</td>
       <td>oss_pipe task exec task.yml</td>
	</tr>
    <tr>
	   <td>analyze</td>
	   <td>分析远端文件尺寸占比 </td>
       <td>&lt;taskfile&gt;</td>
       <td>oss_pipe task analyze task.yml</td>
	</tr>    
</table>


### template
template 子命令用户生成
<table>
	<tr>
	    <th colspan="4">template</th>
	</tr >
	<tr>
	    <td >命令</td>
	    <td>子命令 </td>
	    <td>描述</td>  
        <td>示例</td>  
	</tr >
	<tr >
	    <td rowspan="4">transfer</td>
        <td >oss2oss</td>
        <td >用于生成文件传输类，对象存储到对象存储场景模版</td>
        <td >oss_pipe template transfer oss2oss</td>    
	</tr>
  	<tr>
	    <td>oss2local</td>
	    <td>用于生成文件传输类，对象存储到本地存储场景模版</td>
       <td>oss_pipe template transfer oss2local</td>
	</tr>
  	<tr>
	    <td>local2oss</td>
	    <td>用于生成文件传输类，本地存储到对象存储场景模版</td>
       <td>oss_pipe template transfer local2oss</td>
	</tr>
    <tr>
	    <td>local2local</td>
	    <td>用于生成文件传输类，本地存储到本地存储场景模版</td>
       <td>oss_pipe template transfer local2local</td>
	</tr>
	<tr>
	    <td>compare</td>
	    <td></td>
       <td>校验任务模版(开发中)</td>
	</tr>
</table>

### parameters
parameters 展示 oss_pipe 支持的对象存储供应商，任务类型等信息
<table>
	<tr>
	    <th colspan="3">parameters</th>
	</tr>
    <tr>
	  <td >命令</td>
	  <td>描述  </td>
      <td>示例</td>  
	</tr >
    <tr>
	   <td>provider</td>
	   <td>显示支持的对象存储提供方</td>
       <td>oss_pipe parameters provider</td>
	</tr>
    <tr>
	   <td>task_type</td>
	   <td>显示任务类型 </td>
       <td>oss_pipe parameters task_type</td>
	</tr>    
</table>

### gen_file
gen_file 命令，用于生成文件
<table>
	<tr>
	    <th colspan="4">gen_file</th>
	</tr >
    <tr>
	  <td>命令</td>
	  <td>参数</td>
      <td>描述</td>  
      <td>示例</td>  
	</tr >
    <tr>
	   <td rawspan="3">gen_file</td>
       <td>&lt;file_size&gt;</td>
	   <td>文件大小</td>
       <td rawspan="3">oss_pipe gen_file 10m 1m /root/testfile</td>
	</tr>
    <tr>    	
       <td></td>	 
       <td>&lt;chunk_size&gt;</td> 
       <td>生成文件时每个写入块大小</td>
       <td></td>
	</tr>
    <tr>    	
       <td></td>	 
       <td>&lt;file_name&gt;</td> 
       <td>文件路径</td>
       <td></td>
	</tr>     
</table>


### gen_files
gen_files 命令，用于批量生成文件。
<table>
	<tr>
	    <th colspan="4">gen_files</th>
	</tr >
    <tr>
	  <td>命令</td>
	  <td>参数</td>
      <td>描述</td>  
      <td>示例</td>  
	</tr >
    <tr>
	   <td rawspan="3">gen_files</td>
       <td>&lt;dir&gt;</td>
	   <td>生成批量文件的目录</td>
       <td rawspan="3">oss_pipe gen_files /tmp 3 10m 1m 10</td>
	</tr>
    <tr>    	
       <td></td>	 
       <td>&lt;file_prefix_len&gt;</td> 
       <td>文件前缀长度</td>
       <td></td>
	</tr>
    <tr>    	
       <td></td>	 
       <td>&lt;file_size&gt;</td> 
       <td>文件尺寸</td>
       <td></td>
	</tr>
    <tr>    	
       <td></td>	 
       <td>&lt;chunk_size&gt;</td> 
       <td>生成文件时每块尺寸</td>
       <td></td>
	</tr>
    <tr>    	
       <td></td>	 
       <td>&lt;file_quantity&gt;</td> 
       <td>生成文件数量</td>
       <td></td>
	</tr>      
</table>

### tree
tree 命令，显示所有命令及子命令的树形关系图

### exit
exit 命令，再交互模式下退出程序
