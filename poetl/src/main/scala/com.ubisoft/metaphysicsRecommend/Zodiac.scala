package com.ubisoft.metaphysicsRecommend

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

object Zodiac {
  val ancient_chinese_hour_list = List("子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥")
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
    println("startTime:"+startTime)
    println("yesterday:"+yesterday)
    val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
    /**
      * 建立spark Session
      */
    val spark = SparkSession.builder()
      .appName("zodiac")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val dosql = String.format("select date_hour_fk,user_fk user_id,product_platform_fk,game_platform_fk,profit,valid_monetary from ibds_dw.t_users_bet_hourly_fact where " +
      "  dt >= '%s' and dt <= '%s' and game_kind_fk in (5,8)",startTime,yesterday)
    val dosql_2 = String.format("select user_id,chinese_zodiac,zodiac from ibds_dw.t_users_dim where dt = '%s'",yesterday)
    val t_date_hourly_dim_df = spark.sql("select date_hour_sk date_hour_fk,month_num,hour_num from ibds_dw.t_date_hourly_dim")
    val df = spark.sql(dosql).join(t_date_hourly_dim_df,Seq("date_hour_fk"))
    val df2 = spark.sql(dosql_2)
    val df3 = df.join(df2,Seq("user_id"),"left_outer")
      .rdd.map(x =>Tuple2(x.getString(2)+"|"+x.getString(3)+"|"+x.getInt(6)+"|"+x.getInt(7)+"|"+x.getString(8)+"|"+x.getString(9)
      ,Tuple2(x.getFloat(4),x.getFloat(5)))).groupByKey().map(x => func(x,sysTime,yesterday)).toDF("product_platform_sk","game_platform_sk","zodiac","chinese_zodiac","month"
      ,"ancient_chinese_hour","return_rate","product_id","load_time","snapshot_date")
    df3.repartition(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_game_electronic_zodiac_daily_fact")
    println("finish-----------------------------")
  }
  def func(x: (String, Iterable[(Float, Float)]),load_time:String,snapshot_date:String)={
    var sum_profit = 0.0
    var sum_valid_account = 0.0
    for(tuple <- x._2){
      sum_profit += tuple._1;sum_valid_account += tuple._2
    }
    var return_rate:Option[Float] =None
    if(sum_valid_account != 0){return_rate = Option((sum_profit/sum_valid_account).toFloat)}
    val product_platform_sk = x._1.split("\\|")(0)
    val game_platform_sk = x._1.split("\\|")(1)
    val month = x._1.split("\\|")(2).toInt
    val ancient_chinese_hour =matchHour(x._1.split("\\|")(3).toInt)
    val chinese_zodiac = x._1.split("\\|")(4)
    val zodiac = x._1.split("\\|")(5)
    val product_id = product_platform_sk.split("_")(0)
    (product_platform_sk,game_platform_sk,zodiac,chinese_zodiac,month,ancient_chinese_hour,return_rate,product_id,load_time,snapshot_date)
  }
  def matchHour(x: Int): String = x match {
    case 23 => ancient_chinese_hour_list(0)
    case 0 => ancient_chinese_hour_list(0)
    case 1 => ancient_chinese_hour_list(1)
    case 2 => ancient_chinese_hour_list(1)
    case 3 => ancient_chinese_hour_list(2)
    case 4 => ancient_chinese_hour_list(2)
    case 5 => ancient_chinese_hour_list(3)
    case 6 => ancient_chinese_hour_list(3)
    case 7  => ancient_chinese_hour_list(4)
    case 8 => ancient_chinese_hour_list(4)
    case 9 => ancient_chinese_hour_list(5)
    case 10 => ancient_chinese_hour_list(5)
    case 11 => ancient_chinese_hour_list(6)
    case 12 => ancient_chinese_hour_list(6)
    case 13 => ancient_chinese_hour_list(7)
    case 14 => ancient_chinese_hour_list(7)
    case 15 => ancient_chinese_hour_list(8)
    case 16 => ancient_chinese_hour_list(8)
    case 17 => ancient_chinese_hour_list(9)
    case 18 => ancient_chinese_hour_list(9)
    case 19 => ancient_chinese_hour_list(10)
    case 20 => ancient_chinese_hour_list(10)
    case 21 => ancient_chinese_hour_list(11)
    case 22 => ancient_chinese_hour_list(11)
    case _ => "其他"
  }

}
