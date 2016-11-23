package streaming.king.rest.output

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
  * @author xiaguobing
  * @version 2016-06-17
  **/
class SQLJdbcOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLJdbcOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def url = {
    config[String]("url", _configParams)
  }

  def mode = {
    config[String]("mode", _configParams)
  }

  def dbtable = {
    config[String]("dbtable", _configParams)
  }

  def cfg = {
    val _cfg = _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
    val props: java.util.Properties = new java.util.Properties();
    props.putAll(_cfg - "url" - "mode" - "dbtable");
    props
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult.get(0).asInstanceOf[DStream[String]]
    val func = params.get("_func_").asInstanceOf[(RDD[String]) => DataFrame]
    val _url = url.get
    val _mode = mode.get
    val _dbtable = dbtable.get
    val _cfg = cfg
    dstream.foreachRDD { rdd =>
      try {
        val df = func(rdd)
        df.write.mode(SaveMode.valueOf(_mode)).jdbc(_url, _dbtable, _cfg)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    params.remove("sql")
    List()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    if (url.isDefined && dbtable.isDefined && mode.isDefined) (true, "")
    else
      (false, s"Job name = ${params("_client_")}, Compositor=SQLJdbcOutputCompositor,Message = Both url,dbtable,mode required")
  }
}