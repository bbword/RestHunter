# RestHunter
The project can fetch datas from RESTful API
## RestHunter的REST调用流程
1. 将RestHunter作为一个SparkStreaming的应用启动
2. RestHunter应用读取保存在ES集群中指定索引的REST元数据
3. RestHunter应用对每个元数据中REST API进行预解析,元数据的设置调用REST API
4. RestHunter应用将返回的结果进行解析，将结果对应到指定名称，并保存

注：RestHunter依赖streamingpro，如何启动一个streamingpro的应用详见streamingpro相关文档

RestHunter的策略配置文件
```
{
	"RestMonitorEtl": {
		"desc": "RestMonitorEtl",
		"strategy": "streaming.core.strategy.SparkStreamingStrategy",
		"algorithm": [],
		"ref": [],
		"compositor": [
			{
				"name":"com.letvcloud.bigdata.spark.source.EsInputCompositor",
				"params":[
					{
						"es.nodes":"ip103:9200,ip105:9200,ip50:9200",
						"es.resource":"monitor_db_rest/rest"
					}
				]
			},
			{
				"name": "streaming.king.rest.transform.RestFetchCompositor",
				"params": [
					{
						"resultKey": "result",
						"keyPrefix": "key_"
					}
				]
			},
			{
				"name": "streaming.king.rest.transform.JSonExtractCompositor",
				"params": [
					{
						"resultKey": "result",
						"keyPrefix": "metrics",
						"flat":"false"
					}
				]
			},
			{
				"name": "com.letvcloud.bigdata.spark.transform.RestResultSerializeCompositor",
				"params": [
					{
						"resultKey": "result",
						"keyPrefix": "key_"
					}
				]
			},
			{
				"name": "com.letvcloud.bigdata.spark.output.KafkaOutputCompositor",
				"params": [
					{
						"server":"127.0.0.1:9091",
						"topic":"rest-topic"
					}
				]
			}
		],
		"configParams": {
		}
	}
}
```


# Example：通过RestHunter获取ES集群的运行状态
通过ES的cat api 获取ES集群的监控数据。ES 的cat API 相对简单，无需嵌套获取每个REST结果就可以完成对ES集群的监控数据获取 
## 建立ES REST 元数据，
在ES集群中添加一个可以保存元数据的索引
```
curl -XPUT localhost:9200/monitor_db_rest/ -d '{
  "settings": {
    "index": {
      "number_of_shards": 5,
      "number_of_replicas": 1
    }
  },
  "mappings": {
    "rest": {
      "dynamic_templates": [
        {
          "string_default": {
            "mapping": {
              "index": "not_analyzed",
              "type": "string"
            },
            "match_mapping_type": "string",
            "match": "*"
          }
        }		
      ],
	  "properties": {
      "id": {
        "index": "not_analyzed",
        "type": "string"
      },
      "metrics": {
        "index": "not_analyzed",
        "type": "string"
      },
      "apptype": {
        "index": "not_analyzed",
        "type": "string"
      },
      "logtype": {
        "index": "not_analyzed",
        "type": "string"
      },
      "params": {
        "index": "not_analyzed",
        "type": "string"
      },
      "headers": {
        "index": "not_analyzed",
        "type": "string"
      },	  
      "url": {
        "index": "not_analyzed",
        "type": "string"
      },
      "ip": {
        "index": "not_analyzed",
        "type": "string"
      },
      "appname": {
        "index": "not_analyzed",
        "type": "string"
      },
	  "metastat": {
        "index": "not_analyzed",
        "type": "string"
      },
	  "metaref": {
        "index": "not_analyzed",
        "type": "string"
      },
	  "schedule": {
        "type": "integer"
      }, 
      "method": {
        "index": "not_analyzed",
        "type": "string"
      }
    }
    }
  }
 }'
```

## 获取ES集群整体状态
在元数据中添加ES rest api 的元数据

1. 获取ES集群的线程池reject数目，每隔一段时间获取一次，就可以知道最近时间段ES线程池的变化情况
```
        {
        "_index": "monitor_db_rest",
        "_type": "rest",
        "_id": "localcluster_05",
        "_score": 0,
        "_source": {
          "method": "GET",
          "url": "http://localhost:9200/_cat/thread_pool?format=json&h=ip,bulk.rejected,index.rejected,search.rejected",
          "appname": "localcluster",
          "id": "localcluster_05",
          "logtype": "REST",
          "apptype": "ES",
          "metrics": "key_esthread_bulk_rejected:$..['bulk.rejected'],key_esthread_index_rejected:$..['index.rejected'],key_esthread_search_rejected:$..['search.rejected'],key_ip:$..['ip']"
        }
      },
```  
   
2. 获取ES集群的整体状态 ，每隔一段时间获取一次，就可以监控ES集群是健康状态
```
      {
        "_index": "monitor_db_rest",
        "_type": "rest",
        "_id": "localcluster_03",
        "_score": 0,
        "_source": {
          "method": "GET",
          "url": "http://localhost:9200/_cat/health?format=json&h=status,node.total",
          "appname": "localcluster",
          "id": "localcluster_03",
          "logtype": "REST",
          "apptype": "ES",
          "metrics": "key_es_status:$.[0].['status'],key_es_node_total:$.[0].['node.total']"
        }
      },
```      
3. 获取ES集群的整体状态 ，每隔一段时间获取一次，就可以监控ES集群内存使用情况、负载情况、搜索相关的状态
```      
      {
        "_index": "monitor_db_rest",
        "_type": "rest",
        "_id": "localcluster_04",
        "_score": 0,
        "_source": {
          "method": "GET",
          "url": "http://localhost:9200/_cat/nodes?format=json&h=ip,heap.percent,ram.percent,load,indexing.index_current,search.query_current,search.fetch_current",
          "appname": "localcluster",
          "id": "localcluster_04",
          "logtype": "REST",
          "apptype": "ES",
          "metrics": "key_esnode_query_current:$..['search.query_current'],key_esnode_load:$..['load'],key_esnode_heap_percent:$..['heap.percent'],key_esnode_index_current:$..['indexing.index_current'],key_esnode_ram_shards:$..['ram.percent'],key_ip:$..['ip'],key_esnode_fetch_current:$..['search.fetch_current']"
        }
      },
```
4. 获取ES集群每个节点的磁盘使用情况、shard数目，每隔一段时间获取一次，就可以监控ES集群每个节点的磁盘使用情况
```     
      {
        "_index": "monitor_db_rest",
        "_type": "rest",
        "_id": "localcluster_02",
        "_score": 0,
        "_source": {
          "method": "GET",
          "url": "http://localhost:9200/_cat/allocation?format=json&h=ip,disk.percent,shards",
          "appname": "localcluster",
          "id": "localcluster_02",
          "logtype": "REST",
          "apptype": "ES",
          "metrics": "key_disk_percent:$..['disk.percent'],key_es_shards:$..['shards'],key_ip:$..['ip']"
        }
      },
```    
5. 获取ES集群的在库文档数  ,每隔一段时间获取一次，通过两次在库记录数，可以粗略计算出ES入库QPS
```      
      {
        "_index": "monitor_db_rest",
        "_type": "rest",
        "_id": "localcluster_01",
        "_score": 0,
        "_source": {
          "method": "GET",
          "url": "http://localhost:9200/_cat/count?format=json&h=ip,count",
          "appname": "localcluster",
          "id": "localcluster_01",
          "logtype": "REST",
          "apptype": "ES",
          "metrics": "key_escluster_count:$.[0].['count']"
        }
      }
```
