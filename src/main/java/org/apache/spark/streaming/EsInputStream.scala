package org.apache.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.elasticsearch.spark._

import scala.reflect.ClassTag


class EsInputStream[T: ClassTag](_ssc: StreamingContext, _esResource: String, _esQuery: String, _esParams: Map[String, String])
  extends InputDStream[T](_ssc) {

  def start() {}

  def stop() {}


  def compute(validTime: Time): Option[RDD[T]] = {

    logInfo("Computing RDD for time " + validTime)

    val rdd: RDD[Map[String, AnyRef]] = ssc.sc.esRDD(_esResource, _esQuery, _esParams).
      map(f => f._2.toMap)

    val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "")

    val inputInfo = StreamInputInfo(id, rdd.count(), metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    Some(rdd.asInstanceOf[RDD[T]])
  }
}
