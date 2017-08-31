DataX 是一个异构数据源离线同步工具，致力于实现包括关系型数据库(MySQL、Oracle等)、HDFS、Hive、MaxCompute(原ODPS)、HBase、FTP等各种异构数据源之间稳定高效的数据同步功能。

`datax-elasticsearch`是datax的一个支持elasticsearch的插件，包含`elasticsearchreader`和`elasticsearchwriter`两部分。借助于datax体系，能够高效的在elasticsearch与oracle、mysql、HDFS等异构数据源间同步数据。

**插件安装**

方式一：将下载的插件解压，elasticsearchreader拷贝到`${DATAX_HOME}\plugin\reader`目录，elasticsearchwriter拷贝到`${DATAX_HOME}\plugin\writer`目录。

方式二：clone源码，使用maven编译打包，将打包后的资源放到如下结构中。

```
${DATAX_HOME}
|-- bin       
|   `-- datax.py
|-- conf
|   |-- core.json
|   `-- logback.xml
|-- lib
|   `-- datax-core-dependencies.jar
`-- plugin
    |-- reader
    |   `-- elasticsearchreader
    |       |-- libs
    |       |   `-- elasticsearchreader-plugin-dependencies.jar
    |       |-- elasticsearchreader-0.0.1.jar
    |       `-- plugin.json
    `-- writer
        |-- elasticsearchwriter
        |   |-- libs
        |   |   `--elasticsearchwriter-plugin-dependencies.jar
        |   |-- elasticsearchwriter-0.0.1.jar
        |   `-- plugin.json
        |-- oceanbasewriter
        `-- odpswriter
```
elasticsearchreader和elasticsearchwriter都是同一个jar包，只是入口不同，datax通过plugin.json中定义的入口反射得到实例。elasticsearchreader和elasticsearchwriter的plugin.json内容在源码的resources文件夹中，分别是plugin_reader_template.json和plugin_writer_template.json。修改命名再放到datax对应文件夹中即可。

**elasticsearchreader**

elasticsearchreader任务配置如下
```json
{
	"name": "elasticsearchreader",
	"parameter": {
		"connection": [
			"10.43.164.113:9300"
		],
		"index":"test2",
		"type":"game",
		"pageSize":100,
		"column": ["_id","contact_order_id"]//可以配置成["*"]表示取所有列
	}
}
```
* connection：TransportClient客户端连接地址，可以配置多个地址。
* index：索引
* type：类型
* pageSize：一次请求时每个分片返回的数量。
* column：读取的字段名，可以指定具体的列名也可以配置成["\*"]。指定具体列名时，如果需要读取文档id，列名指定为`"_id"`，如果配置的是["\*"]，文档id放在record的第一个位置。

**elasticsearchwriter**

elasticsearchwriter任务配置如下：
```json
{
	"name": "elasticsearchwriter",
	"parameter": {
		"connection": [
			"10.43.164.113:9300"
		],
		"index":"test2",
		"type":"game",
		"bulkNum":1000,
		"refresh":false,
		"idField":"_id",
		"column": ["_id","contact_order_id","name"]
	}
}
```
connection、index、type、column同上。
* bulkNum：批处理缓冲数量，读取的记录达到一定数量时再一次性写入elasticsearch，适当的bulkNum值能提高性能。
* refresh：是否在一次批处理完成后立即刷新，如果为true，插入的数据能立即被检索到，如果为false，需要等到elasticsearch自动刷新后(约3s)才能被检索到。
* idField：使用指定字段值(不能有重复值)作为es的文档id，不设置或空时默认使用自增长id，如果如上例使用`"_id"`列值作为文档id,`"_id"`列本身不会成为文档的一个字段。如果使用非`"_id"`列如`contact_order_id`作为文档id，`contact_order_id`仍然会成为文档的一个字段(只有`"_id"`才会被特殊处理)。
> 注意：如果不指定idField配置或为空，column中不允许出现elasticsearch的内置字段名

**完整示例**

向elasticsearch写入数据时一定要根据elasticsearch运行环境控制写入速度。否则可能会报一下错误操作写入失败
```
org.elasticsearch.common.util.concurrent.EsRejectedExecutionException: rejected execution of org.elasticsearch.transport.TransportService$4@12748abb on EsThreadPoolExecutor[bulk, queue capacity = 50, org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@29b145ab[Running, pool size = 4, active threads = 4, queued tasks = 50, completed tasks = 31817]]
```
* oracle => elasticsearch
```json
{
   "job": {
       "setting": {
           "speed": {
               "channel": 5,
               "record": 10000
           }
       },
       "content": [
           {
               "reader": {
                   "name": "oraclereader",
                   "parameter": {
                        "username": "root",
                        "password": "1234567890",
                        "splitPk": "game_id",
                        "column": [
                            "game_id","play_time","game_name","col1"
                        ],
                        "connection": [
                            {
                                "table": [
                                    "record"
                                ],
                                "jdbcUrl": [
                                    "jdbc:oracle:thin:@ip:port:db"
                                ]
                            }
                        ]
                   }
               },
               "writer": {
                    "name": "elasticsearchwriter",
                    "parameter": {
                        "connection": ["ip:9300"],
                        "index":"test2",
                        "type":"record2",
                        "bulkNum":10000,
                        "idField":"game_id",
                        "column": ["game_id","play_time","filed3","filed4"]
                    }
               }
           }
       ]
   }
}
```
* elasticsearch => elasticsearch
```
{
   "job": {
       "setting": {
           "speed": {
               "channel": 5,
               "record": 10000
           }
       },
       "content": [
           {
               "reader": {
                   "name": "elasticsearchreader",
                   "parameter": {
                        "connection": ["ip1:9300"],
                        "index":"test2",
                        "type":"record",
                        "pageSize":100,
                        "column": ["_id","game_id","play_time","filed3","filed4"]
                   }
               },
               "writer": {
                    "name": "elasticsearchwriter",
                    "parameter": {
                        "connection": ["ip2:9300"],
                        "index":"test2",
                        "type":"record2",
                        "idField":"_id",
                        "column": ["_id","game_id","play_time","filed3","filed4"]
                    }
               }
           }
       ]
   }
}
```

