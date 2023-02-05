import com.mashape.unirest.http.{HttpResponse, Unirest}

object rest_test{



  def main(args: Array[String]): Unit = {

    var get:HttpResponse[String]= Unirest.get("https://jsonplaceholder.typicode.com/comments/").asString();
    println(get.getBody)



  }
}

