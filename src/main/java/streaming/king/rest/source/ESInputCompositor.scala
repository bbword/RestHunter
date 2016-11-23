package streaming.king.rest.source

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.EsInputStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


/**
  * @author xiaguobing
  * @version 2016-06-17
  **/

class ESInputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[ESInputCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  private def esParams = {
    _configParams.get(0).filter {
      f =>
        if (f._1 == "es.resource") false else true
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
    val ssc = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime].streamingContext
    List((new EsInputStream[Map[String, AnyRef]](ssc, esResource, esQuery, esParams)).asInstanceOf[T])
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    val esResource: Option[String] = config[String]("es.resource", _configParams)
    (esResource.isDefined, "please set es.resource to save")
  }
}
