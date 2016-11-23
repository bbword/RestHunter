package streaming.king.rest.service

import com.google.inject.{Inject, Singleton}
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.common.settings.Settings
import net.csdn.modules.http.RestRequest
import net.csdn.modules.transport.HttpTransportService
import net.liftweb.json.JsonAST.JValue
import net.sf.json.{JSONArray, JSONObject}

import scala.collection.JavaConversions._

/**
  * @author xiaguobing
  * @version 2016-06-17
  **/
@Singleton
class ESService @Inject()(settings: Settings,
                          transportService: HttpTransportService
                         ) extends JSONHelper {

  val hostAndPort = settings.get("es.nodes").split(",").head
  val resource = settings.get("es.resource", "monitor_db_rest/rest")
  val queryUrl = new Url(s"http://${hostAndPort}/_sql")
  val saveUrl = new Url(s"http://${hostAndPort}/${resource}")

  def list(sql: String) = {

    val response = transportService.get(queryUrl, WowCollections.map("sql", sql).asInstanceOf[java.util.Map[String, String]])
    import net.liftweb.json._
    implicit val formats = net.liftweb.json.DefaultFormats
    val json = parse(response.getContent)
    val result = json \ "hits" \ "hits"
    sourceMap(result)
  }

  def listWithPage(sql: String, from: Long, size: Long) = {
    val response = transportService.get(queryUrl, WowCollections.map("sql", s"${sql} limit ${from},${size}").asInstanceOf[java.util.Map[String, String]])
    import net.liftweb.json._
    implicit val formats = net.liftweb.json.DefaultFormats
    val json = parse(response.getContent)
    val result = json \ "hits" \ "hits"
    val total = json \ "hits" \ "total"
    sourceMap(result, total)
  }

  def jsave(item: java.util.Map[String, String]) = {
    val id = item("id").asInstanceOf[String]
    val url = new Url(s"${saveUrl}/$id")
    val response = transportService.http(url, toJsonMap4J(item), RestRequest.Method.PUT)
    response != null && response.getStatus == 200
  }


  def jdelete(id: String) = {
    val url = new Url(s"${saveUrl}/$id")
    val response = transportService.http(url, null, RestRequest.Method.DELETE)
    response != null && response.getStatus == 200
  }


}

trait JSONHelper {

  import net.liftweb.{json => SJSon}

  def parseJson[T](str: String)(implicit m: Manifest[T]) = {
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(str).extract[T]
  }


  def sourceMap(jSource: JValue) = {
    val lSource = jSource.values.asInstanceOf[List[Map[String, Any]]]
    val result: List[Map[String, Any]] = lSource.map {
      f => f("_source").asInstanceOf[Map[String, Any]]
    }
    result
  }

  def sourceMap(jSource: JValue, jTotal: JValue) = {
    val lSource = jSource.values.asInstanceOf[List[Map[String, Any]]]
    val lTotal = jTotal.values.asInstanceOf[BigInt].toLong
    val result: List[Map[String, Any]] = lSource.map {
      f => f("_source").asInstanceOf[Map[String, Any]]
    }
    (result, lTotal)
  }

  def toJsonStr(item: AnyRef) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.write(item)
  }

  def toJsonList4J(item: Object) = {
    JSONArray.fromObject(item).toString
  }

  def toJsonMap4J(item: Object) = {
    JSONObject.fromObject(item).toString
  }
}
