package streaming.king.rest.transform

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._
import scala.collection.Map

/**
  * @author xiaguobing
  * @version 2016-06-17
  **/
class LogTransformerExample[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[LogTransformerExample[T]])

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

    val _keyPrefix = keyPrefix

    val stream: DStream[String] = mrs.map { f =>
      val sb = new StringBuffer()
      val url = f.get("url")
      val restMethod = f.get("method")
      val ip = f.get("ip")
      val logType = f.get("logtype")
      val appName = f.get("appname")
      val params = f.get("params")
      for (u <- url; m <- restMethod; i <- ip; l <- logType; a <- appName) {
        sb.append("[").append(i)
        sb.append("]\t[").append(l)
        sb.append("]\t[").append(u)
        sb.append("]\t[").append(a)
        sb.append("]\t").append(DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")))
        val httpContent = f.get(_resultKey)
        sb.append(" [INFO] ")
        sb.append(httpContent)
      }
      val sbstring: String = sb.toString
      if (sbstring.size > 1) {
        sbstring
      }
      else
        ""
    }.filter(s => s != null && s.size > 1)
    List(stream.asInstanceOf[T])
  }


}
