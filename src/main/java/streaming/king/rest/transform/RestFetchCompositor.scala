package streaming.king.rest.transform

import java.util

import net.csdn.common.path.Url
import org.apache.http.client.fluent.Request
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._
import scala.collection.Map

/**
  * 5/25/16 WilliamZhu(allwefantasy@gmail.com)
  */
class RestFetchCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[RestFetchCompositor[T]])

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def resultKey = {
    config("resultKey", _configParams).getOrElse("result")
  }

  def keyPrefix = {
    config("keyPrefix", _configParams).getOrElse("key_")
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val mrs = middleResult(0).asInstanceOf[DStream[Map[String, AnyRef]]]
    val _resultKey = resultKey
    val newMrs = mrs.map { f =>
      val res = Request.Get(new Url(f("url").toString).toURI).execute()
      val response = res.returnResponse()
      val content = EntityUtils.toString(response.getEntity)
      if (response != null && response.getStatusLine.getStatusCode == 200) {
        (true, f + (_resultKey -> content))
      } else {
        println(s" Rest API : ${f("url")} fail. Reason:  " +
          s"${if (res == null || res.returnResponse() == null) "network error" else content}")
        (false, f)
      }
    }.filter(f => f._1).map(f =>
      f._2.toMap
    )
    List(newMrs.asInstanceOf[T])
  }
}
