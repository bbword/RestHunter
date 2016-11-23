package streaming.king.rest.transform

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.king.rest.service.RestFetchUtil

import org.elasticsearch.spark._
import scala.collection.JavaConversions._


/**
  * @author xiaguobing 20160722
  */
class NestRestHandlerCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[NestRestHandlerCompositor[T]])

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def keyPrefix = {
    config("keyPrefix", _configParams).getOrElse("key_")
  }

  def fetchLevel = {
    config("fetchLevel", _configParams).getOrElse(3)
  }

  private def esParams = {
    _configParams.get(0).filter {
      f =>
        f._1 match {
          case "es.resource" => false
          case "es.query" => false
          case p: String => p.startsWith("es.")
        }
    }.toMap.asInstanceOf[Map[String, String]]
  }

  private def esResource = {
    val esResource: Option[String] = config[String]("es.resource", _configParams)
    require(esResource.isDefined, "please set es.resource to save")
    esResource.get
  }

  private def esQuery = {
    val esQuery: Option[String] = config[String]("es.query", _configParams)
    esQuery.getOrElse("?q=*")
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val mrs = middleResult(0).asInstanceOf[DStream[Map[String, Any]]]
    val _esResource = esResource
    val _esQuery = esQuery
    val _esParams = esParams
    val _fetchLevel = fetchLevel
    //val _sqlContext: SQLContext = sqlContextHolder(params)
    val newMrs = mrs.transform {
      tdd =>
        val rdd1 = tdd.map(f => (f("id").asInstanceOf[String], f))
        val rdd2: RDD[(String, Map[String, Any])] = rdd1.sparkContext.esRDD(_esResource, _esQuery, _esParams).map(f => f._2.toMap).map(f => (f("id").asInstanceOf[String], f))
        val now: DateTime = DateTime.now()
        val rdd: RDD[Map[String, Any]] = rdd1.leftOuterJoin(rdd2).map(f => f._2._1 ++ f._2._2.getOrElse(Map[String, AnyRef]())).filter(f => f("metastat") != "invalid").filter {
          f =>
            val schedule = f("schedule").asInstanceOf[Integer].intValue()
            schedule match {
              case 0 => true
              case -1 => f.get("executeTime").isDefined
              case interval if (interval > 0) => now.getMillis > interval + f.getOrElse("executeTime", 0L).asInstanceOf[Long]
            }
        }.map {
          f =>
            val url: String = f.getOrElse("url", "-").asInstanceOf[String]
            val syncValue: String = f.getOrElse("syncValue", "-").asInstanceOf[String]
            f ++ RestFetchUtil.urlParse(url, syncValue)
        }
        //Save executeTime
        rdd.map {
          f =>
            Map("id" -> f("id"), "executeTime" -> now.getMillis, "syncValue" -> f("syncValue"))
        }.saveToEs(_esResource, _esParams)
        rdd
    }.filter(f => f("metastat") != "invalid").map { f =>
      val ref: String = f("metaref").asInstanceOf[String]
      if (ref == "-") {
        (f("id").asInstanceOf[String], RestFetchUtil.urlRequest(f))
      } else {
        (ref, f ++ Map("request_done" -> false))
      }
    }.filter(f => f._2.nonEmpty)
    var fetchResult: DStream[(String, Map[String, Any])] = newMrs
    for (i <- 1 to _fetchLevel) {
      //支持5层的嵌套
      fetchResult = nestedRestFetch(fetchResult)
    }
    //val validColumns = List("id", "metaref", "metastat", "logtype", "appname", "apptype", "url", "method", "schedule", "output", "key_records", "headers", "params", "metrics")
    //val mapResult: DStream[Map[String, Any]] = fetchResult.map(f => f._2).map(f => f.filter(f1 => validColumns.contains(f1._1)))
    val mapResult: DStream[Map[String, Any]] = fetchResult.map(f => f._2) //.map(f => f.filter(f1 => validColumns.contains(f1._1)))
    List(mapResult.asInstanceOf[T])
  }

  def nestedRestFetch(nestedResult: DStream[(String, Map[String, Any])]) = {
    val _keyPrefix = "key_"
    nestedResult.groupByKey().flatMap { f =>
      val m1: Iterable[Map[String, Any]] = f._2
      val listDone: List[Map[String, Any]] = m1.filter(_ ("request_done").asInstanceOf[Boolean]).toList
      val mPending: List[Map[String, Any]] = m1.filter(!_ ("request_done").asInstanceOf[Boolean]).toList
      if (listDone.isEmpty || mPending.isEmpty) {
        m1.map(m1f => (f._1, m1f)) //全部已经执行的rest，就重新返回:全部未执行的rest，也重新返回
      } else {
        //val mDone: Map[String, Any] = listDone(0) //默认只有一个完成rest
        listDone.flatMap {
          mDone =>
            mPending.flatMap { p =>
              val url = p("url").asInstanceOf[String]
              val keyReplace: Map[String, Any] = mDone.filter(f1 => f1._1.startsWith(_keyPrefix) && url.contains("/:" + f1._1)) //找出要替换的参数
              (List(p) /: keyReplace) {
                (result, param) =>
                  result.flatMap {
                    rp =>
                      val resultUrl = rp("url").asInstanceOf[String]
                      param._2 match {
                        case arr: java.util.List[String] => {
                          arr.map { av =>
                            val newUrl: String = resultUrl.replaceAll(":" + param._1, av)
                            p ++ Map("url" -> newUrl, param._1 -> av) //更新url
                          }.filter(af => af.nonEmpty)
                        }
                        case _ => {
                          val newUrl: String = url.replaceAll(":" + param._1, param._2.toString)
                          List(p ++ Map("url" -> newUrl, param._1 -> param._2.toString)) //更新url
                        }
                      }
                  }
              }.map {
                lp =>
                  RestFetchUtil.urlRequest(lp)
              }.filter(lp => lp.nonEmpty)
            }.map(p1 => (p1("id").asInstanceOf[String], p1))
        } ++ listDone.map(p2 => (p2("id").asInstanceOf[String], p2))
      }
    }
  }
}