package streaming.king.rest.service


/**
  * @author xiaguobing
  * @version 2016-06-17
  **/
object JSONPathExtractor {

  import net.liftweb.json._

  def parse(json: String, path: String) = {

  }

  def findPath(json: JValue, path: String) = {
    var temp = json

    path.split("\\\\").foreach { pathF =>
      temp = temp \ pathF
    }
    val value = temp match {
      case null => "null"
      case JBool(true) => true
      case JBool(false) => false
      case JDouble(n) => n
      case JInt(n) => n
      case JNull => "null"
      case JString(null) => "null"
      case JString(s) => s
      case JArray(arr) => sys.error("can't render 'JArray'")
      case JField(n, v) => sys.error("can't render 'JField'")
      case JObject(obj) => sys.error("can't render 'JObject'")
      case JNothing => sys.error("can't render 'nothing'")
    }
    value
  }
}
