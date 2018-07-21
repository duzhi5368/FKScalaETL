import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

object TestScala {
  var a = 2
  var b = a+9

  def main(args: Array[String]): Unit = {
    val data = "{\"zhangsan\":1111}"
    println(data)
    val jsonParser = new JSONParser()
    val jsonObj: JSONObject = jsonParser.parse(data).asInstanceOf[JSONObject]
    println(jsonObj.get("zhangs"))



    val t1 = (1,2)
    val t2 = (3,4)
    println(a)
    println(b)
    println(add(30))
    def add(x:Int)={
      b+x
    }
  }
  def x(x:Int)={
    println("x function")
  }
  println("hello")

}
