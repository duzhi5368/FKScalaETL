package scala.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object Test {
  import scala.collection.mutable.Map
  val map:Map[String,String] = Map()
  def main(args: Array[String]): Unit = {
    val x = "2018-12-15 02:22:55"
    val y = x.split(" ")(0) + "-" + x.split(" ")(1).split(":")(0)
    map(y) = "ddd"
    println(map.get(y))
    //
//    val s  = "step2_aaa_bbb"
//    val i  = s.split("_")(1)
//    println(i)










//    val a:ArrayBuffer[String]= mutable.ArrayBuffer.empty
//     a += "'aa"
//    var a,b,c = "aa"
//    try {
//      println(a.toInt)
//    }catch {case e:Exception =>{}}



//    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
   map("a") = "aa"
        map("b") = "bb"
        println(map("a"))




//    if(a.contains("河南"))
//    println(a)
//    breakable{
//      while (true){
//        println("true")
//        break()
//      }
//    }

//    case class BankMapper(user_fk:String, product_id:String, bank:String, country:String, province:String, area:String, branch:String, priority_order:Int,
//                          load_time:String, dt:String)
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark Test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.textFile("src/test/bank.csv").map(_.split(",")).map(a => Tuple2(a(0),a(1)+","+a(2)))
      .flatMapValues(_.split(",")).toDF("name","age").show()



    //    val arr = Array("11",2,3)
//    println(arr)
////    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
//    spark.sql("use ibds_staging")
//    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    val testDF = spark.sql("select * from t_orders  limit 100").write.mode("overwrite").saveAsTable("ibds_dw.sheltontest")
//    val fix  = (x:Int) => x+2
//    val  fixUDF  = udf(fix)
//    testDF.withColumn("sum",fixUDF(testDF("age"))).show()

//    testDF.rdd.map(
//      a=>{println(a.getClass.getName+"bbb");
//        (a.getAs[String](0),a.getAs[String](2))}).toDF().show()
//    testDF.createOrReplaceTempView("test")
//    val df2 = spark.sql("select  sum(age)  from  test").show()
//    testDF.dropDuplicates(Seq("country")).show()
//    var a = df2.collectAsList()
    //   df2.select("billno")
    //   df2.show()
    //   testDF.select("billno").write.format("csv").saveAsTable("src/main/resources/res2.txt")
    //   val a = 1.toFloat
    //   var c = a+b
    //   println(t)
    //   println(t+100l)
    /**
      * get monetary
      */
//    var  monetary:BigDecimal = 0;
//    val df1 = testDF.groupBy("billno","loginname").count()
//    df1.join(df1,Seq("billno","loginname")).orderBy($"billno".desc,$"loginname".desc).show()
  }
}
