package org.apache.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamInputInfo

import scala.reflect.ClassTag


class JdbcInputStream[T: ClassTag](_sqlContext: SQLContext, _ssc: StreamingContext, _jdbcParams: Map[String, String])
  extends InputDStream[T](_ssc) {

  def start() {}

  def stop() {}


  def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)
    val df = _sqlContext.read.format("jdbc").options(_jdbcParams).load()
    val rdd: RDD[String] = df.toJSON.rdd
    val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "")
    val inputInfo = StreamInputInfo(id, rdd.count(), metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    Some(rdd.asInstanceOf[RDD[T]])
  }
}
