import org.json4s.jackson.Serialization
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Map
object axon_test {
  def get_axon_token(axon_host_url: String): String= {
    val axon_login_url = f"https://$axon_host_url/api/login_check"
    val json_map = Map(
      "username"-> "modak_dev_services@bms.com",
      "password"-> ""
    )
    implicit val formats = org.json4s.DefaultFormats
    val json_string = Serialization.write(json_map)
    val headers = Map(
      "Content-Type"-> "application/json"
    )
    val response = requests.post(axon_login_url, data=json_string, headers=headers)
    println(response.statusCode)
    val parsed_response = parse(response.text()).extract[Map[String, String]]
    println(parsed_response("token"))
    return parsed_response("token")
  }
  def update_axon(axon_host_url: String, edc_host_url: String, Id: String, database_name: String, stampAvailableInCDPOnAxon: Boolean, stampCdpDbUrlOnAxon: Boolean): Unit = {
    val axon_token = get_axon_token(axon_host_url)
    println(axon_token)
    val object_url = f"https://$axon_host_url/api/v1/object"
    println(object_url)
    val json_data = Map(
      "type"-> "Data Set",
      "object" -> Map(
        "ID"-> Id
      ))
    println(json_data)
    if (stampCdpDbUrlOnAxon) {
      val edc_metadata_consumption_path = "https://"+edc_host_url+"/ldmcatalog/main/ldmObjectView/('$obj':'CDP_Hive_Catalog:___Hive%20Metastore/"+database_name+"','$type':com.infa.ldm.relational.Schema,'$where':ldm.ThreeSixtyView)"
      println("=======")
      println(edc_metadata_consumption_path)
      val obj = json_data.get("object") match {
        case Some(b: Map[String, Any]) => b + ("EDC Consumption Metadata Path" -> edc_metadata_consumption_path)
      }
      json_data("object") = obj
    }
    if (stampAvailableInCDPOnAxon) {
      val obj = json_data.get("object") match {
        case Some(b: Map[String, Any]) => b + ("Available in CDP" -> "Yes")
      }
      json_data("object") = obj
    }
    println(json_data)
    implicit val formats = org.json4s.DefaultFormats
    val json_string = Serialization.write(json_data)
    println("=====json_string")
    println(json_string)
    val bearer_token ="Bearer "+axon_token
    val headers = Map(
      "Authorization" -> bearer_token,
      "Content-Type"-> "application/json"
    )
    val response = requests.put(object_url, data = json_string, headers = headers)
    println(response.statusCode)
  }
  def main(args: Array[String]): Unit = {
    //    val r = requests.get("https://dev-edlng-na-dl-gateway.dev-edln.zys7-skee.cloudera.site/dev-edlng-na-dl/cdp-proxy-api/atlas/api/atlas/v2/search/basic?query=ref_edl_celabs_labware_refined_celabs_labware_dataset_sensitive_us.diag&excludeDeletedEntities=true&typeName=hive_table&includeClassificationAttributes=true", auth = ("srv_mc-dev-nabu-poc", "Bmsnabu@321"))
    //    val responseText = r.text
    //    print(responseText)
    val stampCdpDbUrlOnAxon = true
    val stampAvailableInCDPOnAxon = true
    val database_name = "test_dataset_31jan"
    val edc_host_url = "edccatalog-dev.web-dev.bms.com:9085"
    val Id = "11659"
    val axon_host_url = "axon-dev.web-dev.bms.com:9443"
    update_axon(axon_host_url, edc_host_url, Id, database_name, stampAvailableInCDPOnAxon, stampCdpDbUrlOnAxon)
  }
}