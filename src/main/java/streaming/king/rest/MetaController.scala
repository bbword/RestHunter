package streaming.king.rest

import com.google.inject.Inject
import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.http.{ApplicationController, ViewType}
import org.apache.http.client.fluent.Request
import streaming.king.rest.service._

import scala.collection.JavaConverters._

/**
  * 5/25/16 WilliamZhu(allwefantasy@gmail.com)
  */
class MetaController @Inject()(esService: ESService) extends ApplicationController {

  @At(path = Array("/resthunter/list"), types = Array(GET))
  def list = {

    cPaginate
    val items = esService.list(s"select * from ${esService.resource}")
    paginate.totalItems(items.size)
    renderHtml(200, "/rest/ace.vm", WowCollections.map(
      "feeds", items
    ))
  }

  @At(path = Array("/resthunter/metalist"), types = Array(GET))
  def metalist = {
    val page = paramAsLong("page", 1)
    val rows = paramAsLong("rows", 10)
    var sidx = param("sidx", "appname")
    if (sidx == "") sidx = "appname"
    val sort = param("sord", "")
    val from = rows * (page - 1)
    val list = esService.listWithPage(s"select * from ${esService.resource} order by ${sidx} ${sort}", from, rows)
    val mapResponse: java.util.Map[String, Any] = Map("rows" -> list._1.map(_.asJava).asJava, "page" -> page, "records" -> list._2, "total" -> Math.ceil(list._2 / rows.toDouble).toInt, "userdata" -> Map().asJava).asJava
    render(200, mapResponse, ViewType.json)
  }

  @At(path = Array("/resthunter/test"), types = Array(GET))
  def metaTest = {
    val id = param("id", "");
    if (id == "") {
      render(200, "ID is null", ViewType.json)
    } else {
      val list = RestFetchUtil.urlTest(id, esService)
      render(200, list, ViewType.json)
    }
  }

  @At(path = Array("/resthunter/create"), types = Array(POST))
  def postCreate = {
    esService.jsave(params())
    //redirectTo("/resthunter/list", WowCollections.map())
    render(200, "ok", ViewType.json)
  }

  @At(path = Array("/resthunter/delete"), types = Array(GET, POST, DELETE))
  def delete = {
    esService.jdelete(param("id"))
    //redirectTo("/resthunter/list", WowCollections.map())
    render(200, "ok", ViewType.json)
  }

  @At(path = Array("/resthunter/path/validate"), types = Array(GET))
  def pathValidate = {
    val url = param("url")
    val path = param("path")
    val res = Request.Get(new Url(url).toURI).execute().returnContent().asString()
    val value = JSONPath.read(res, path).toString
    render(200, value)
  }

  def cPaginate = {
    paginate = new Paginate(paramAsInt("page", 1), paramAsInt("pageSize", 15))
  }

  var paginate: Paginate = _


}