package streaming.king.rest.source

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{EsInputStream, JdbcInputStream}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


/**
  * 2016/08/24 xiaguobing
  */
class JdbcInputCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[JdbcInputCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def options = {
    _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val ssc = sparkStreamingRuntime(params).streamingContext
    val sqlContext: SQLContext = sqlContextHolder(params)
    List((new JdbcInputStream[String](sqlContext, ssc, options)).asInstanceOf[T])
  }
}
