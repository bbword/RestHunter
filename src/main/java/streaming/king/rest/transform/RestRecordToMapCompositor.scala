package streaming.king.rest.transform

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
  * Created by xiaguobing on 2016/5/6.
  */
class RestRecordToMapCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[RestRecordToMapCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[Map[String, Any]]]
    val newDStream: DStream[util.Map[String, Any]] = dstream.flatMap {
      f =>
        f("key_records") match {
          case mapRecord: util.Map[String, Any] => {
            mapRecord.put("output", f("output"))
            List(mapRecord)
          }
          case arrRecord: util.List[Any] => arrRecord.map {
            r =>
              val map: util.Map[String, Any] = r.asInstanceOf[util.Map[String, Any]]
              map.put("output", f("output"))
              map
          }
        }
    }.map {
      record =>
        if (record.containsKey("date")) {
          val date: Any = record("date")
          record.remove("date")
          record.put("pday", date)
        }
        record
    }
    List(newDStream.asInstanceOf[T])
  }
}
