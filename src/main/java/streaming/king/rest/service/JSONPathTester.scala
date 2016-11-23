package streaming.king.rest.service

import net.csdn.common.path.Url
import org.apache.http.client.fluent.Request
import org.apache.http.util.EntityUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * @author xiaguobing
  * @version 2016-06-17
  **/
object JSONPathTester {
  def parse(maps: List[Map[String, Any]]) = {
    val _resultKey = "result"
    val _keyPrefix = "metrics"
    val _flat = false
    maps.map { f =>
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
    ).flatMap { f =>
      val keyWithPath = f.filter(f => f._1.startsWith(_keyPrefix)).flatMap(f => f._2.asInstanceOf[String].split(",")).map { f => val arr = f.split(":"); (arr(0), arr(1)) }
      //val keyWithPath = f.filter(f => f._1.startsWith(_keyPrefix)).map(f => (f._1, f._2.asInstanceOf[String])).toMap
      val json = f(_resultKey).toString

      val newValue = keyWithPath.map { kPath =>
        val key = kPath._1
        val path = kPath._2
        val value = JSONPath.read(json, path).asInstanceOf[Any]
        (key, value)
      }

      if (_flat) {
        newValue.map { k =>
          f + (k._1 -> k._2)
        }

      } else {
        List(Map[String, Any]() ++ newValue)
      }
    }
  }
}
