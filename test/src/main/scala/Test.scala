import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

object Test {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'")

  var str = "aaa"

  val b = a+2
  val a = 15
  val c = a+b
  def matchHour(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }
  def fun(row: Array[String])={
    val name = row(0)
    val age = row(1).toFloat
    var res:Option[Float] =None
    if(age>1){res = Option(age)}
    (name,res)
  }
  def main(args: Array[String]): Unit = {
    str = "bbb"
//    println("\"")
//    val a = "aaa \" bbb\""
    println(a,b,c)
//    case class Test(name:String,age:Int,var diff:Int)
//    var arr = Iterable(Test("zhangsan",31,0),Test("lisi",20,0)).toList
//    val rows = arr.sortBy(_.age)
//    for (i <- 0 until rows.length){
//      try {
//        val diff =rows(i+1).age - rows(i).age
//        rows(i).diff = diff
//        println(rows(i))
//      }catch {
//        case e:IndexOutOfBoundsException => println(0)
//      }
//    }
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
//    val date1 = dateFormat.parse("2018-09-08T02:03:56.900+08:00")
//    val date2 = dateFormat.parse("2018-09-08T03:03:58.900+08:00")
//    val seconds = (date2.getTime-date1.getTime)/1000.0
//    println(seconds)
//    println(a,b)
//    val map = Map(1 ->"name",2 ->"age")
//    val newmap =map.map(i => (i._1+1,i._2))
//    for(i  <- newmap){println(i)}



//    println(matchHour(1))
//    println("02".toInt)

//
//
//
//    val a = "2018-05"
//    println(a.split("-")(1))
//    println(a.split("\\|")(0))
//
//
//    var age:Option[Float] = Option(1)

//    val bB = List("招", "工", "农", "建", "交", "民", "光", "兴", "浦", "广", "圳", "平", "邮", "夏", "中信", "信", "中国", "比",
//      "BTC", "btc", "CMB", "ABC", "CCB", "BOCOM", "渤海银行", "ICBC")
//    val B = List("招商银行", "工商银行", "农业银行", "建设银行", "交通银行", "民生银行", "光大银行", "兴业银行", "上海浦东发展银行", "广东发展银行", "深圳发展银行", "平安银行",
//      "中国邮政储蓄银行", "华夏银行", "中信银行", "农村信用社", "中国银行", "比特币", "比特币", "比特币", "招商银行", "农业银行", "建设银行", "交通银行",
//      "渤海银行", "工商银行")
//    for(i <- 0 until 3){
//      println(bB(i))
//    }










//    val a = List(1.0,2.0,3.0,4.0)
//    val len = a.length
//    println(a.max)
//    val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
//    println("当前执行日期："+sysTime)
//
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark Test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    spark.sparkContext.textFile("file:\\D:\\IdeaProject\\shelton\\src\\test\\bank.csv")
      .map(_.split(",")).map(a => fun(a)).groupByKey()
      .map(x=>{
      val name = str
      val arr = List(2,5,8)
      (name,arr)
    }).flatMapValues(x=>x)
     .toDF("name","age").show()
//    spark.sparkContext.textFile("file:\\D:\\IdeaProject\\shelton\\src\\test\\bank2.csv").map(_.split(",")).map(a => fun(a))
//     .toDF("name","age").createOrReplaceTempView("temptable2")
//    val df = spark.sql("select a.name,a.age,b.name from temptable1 a  left join temptable2 b on a.name = b.name").cache()
//    df.show()
//    df.rdd.map(func).toDF("a","b","c").show()
//    df.unpersist()














//    val count = df.rdd.map(a =>{
//      var res = a.getAs[String]("b_name")
//      println("res:"+res)
//      res
//    }).count()
//    println("count:"+count)

//    df1.show()
//    val df2 = spark.sparkContext.textFile("src/test/bank2.csv").map(_.split(",")).map(a => Tuple2(a(0),a(1)))
//      .toDF("name","age")
//    df2.show()
//    df1.join(df2,df1("name1") === df2("name"),"full_outer").na.fill(Map("name1" ->"shabi","age" ->10))show()
//    val writer = new PrintWriter(new File("natasha.csv"))
//    writer.write("hello word")
//    writer.write("hello scala")
//    writer.close()
//    val list  = List(Tuple3("a",1,4),Tuple3("a",1,8),Tuple3("b",1,5),Tuple3("b",3,5))
//    val  offser_gmcode = ArrayBuffer[String]()
//    list.map(x =>Tuple2(x._1,x._2)).groupBy(x => x._1).foreach(x =>{
//      if(x._2.contains(Tuple2(x._1,1))  &&  x._2.contains(Tuple2(x._1,2))){
//        println(x._1)
//      }
//    })
//    val continueWinTimes = ArrayBuffer(1,5,8,1,0)
//    val maxContinueWinTimes = continueWinTimes.min
//    println(maxContinueWinTimes)
//    for(i <- 5 to 6 ){
//      println(i+"i")
//    }
//    val res = list.sortBy(_._3)
//    for(i <- res){
//      println(i)
//    }

  }
  def func(x:Row) = {
    val a = x.getString(0)
    val b = x.getFloat(1)
    val c = x.getString(2)
    println(a)
    println(b)
    println(c)
    val res = a+"|"+b+"|"+c
    println(res)
    println(res.split("\\|")(2))
    (x.getString(0),x.getFloat(1),x.getString(2))
  }
  private def add(a:Int,b:Int): Int ={
    a+b
  }


}
