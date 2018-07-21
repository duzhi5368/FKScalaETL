package com.ubisoft.metaphysicsRecommend

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

object T_Users_Electronic_Return_Rate {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  /**
    * 获得输入日期之前N天日期,返回值string类型，格式为"yyyy-MM-dd"
    * @param dt
    * @param n
    * @return
    */
  def getNDayAgo(dt:String,n:Int) = {
    val date=dateFormat.parse(dt)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE,-n)
    dateFormat.format(cal.getTime)
  }
  def main(args: Array[String]): Unit = {
    val yesterday = args(0) //暨snapshot_date
    val nDayAgo = args(1).toInt
    val startTime = getNDayAgo(yesterday,nDayAgo)
    val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
    println("startTime:"+startTime)
    println("yesterday:"+yesterday)
    /**
      * 建立spark Session
      */
    val spark = SparkSession.builder()
      .appName("T_Users_Electronic_Return_Rate")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val dosql = String.format("select user_fk user_id,product_platform_fk,game_platform_fk,profit,valid_monetary from ibds_dw.t_users_bet_hourly_fact where " +
      "  dt >= '%s' and dt <= '%s' and game_kind_fk in (5,8)",startTime,yesterday)
    val df = spark.sql(dosql).select("product_platform_fk","user_id","game_platform_fk","profit","valid_monetary").rdd
      .map(x =>Tuple2(x.getString(0).split("_")(0)+"|"+x.getString(1)+"|"+x.getString(2),Tuple2(x.getFloat(3),x.getFloat(4)))).groupByKey().map(x =>func(x,sysTime,yesterday))
      .toDF("product_id","user_id","game_platform_fk","return_rate","load_time","snapshot_date")
    df.repartition(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_electronic_return_rate")
    println("finish-------------------------------------------")
  }
  def func(x: (String, Iterable[(Float, Float)]),sysTime:String,snapshot_date:String)={
    var sum_profit = 0.0
    var sum_valid_account = 0.0
    for(tuple <- x._2){
      sum_profit += tuple._1;sum_valid_account += tuple._2
    }
    var return_rate:Option[Float] =None
    if(sum_valid_account != 0){return_rate = Option((sum_profit/sum_valid_account).toFloat)}
    (x._1.split("\\|")(0),x._1.split("\\|")(1),x._1.split("\\|")(2),return_rate,sysTime,snapshot_date)
  }

}
