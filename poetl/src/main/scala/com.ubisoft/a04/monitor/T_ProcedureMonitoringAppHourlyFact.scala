package com.ubisoft.a04.monitor

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable

object T_ProcedureMonitoringAppHourlyFact {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'")
  def main(args: Array[String]): Unit = {
    val snapshot_date  = args(0).trim
    val load_time = dateFormat.format(new Date)
    val spark = SparkSession.builder()
      .appName("T_ProcedureMonitoringAppHourlyFact")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val t_date_hourly_dim = spark.sql("select date_hour_sk,hour_num,full_date_hour from ibds_dw.t_date_hourly_dim")
    val df = spark.sql(String.format("select product_id,platform,star_level,hierarchy,tag_name,tag_step,tag_detail,tag_id,page_url,referer," +
      "app_id,app_version,palcode,os_type,os_version,server_country country,server_province province,server_city city," +
      "user_id,session_id,split(regexp_replace(click_time,'T','-'),':')[0] as full_date_hour,t_action,errCode" +
      " from test.t_monitoring_games where snapshot_date = '%s' ",snapshot_date)).join(t_date_hourly_dim,Seq("full_date_hour"),"left_outer")
    df.rdd.map(func).groupByKey().map(x => func2(x,load_time,snapshot_date)).toDF().write.mode(SaveMode.Overwrite).insertInto("test.t_monitor_games_dw")
    spark.close()
  }
  def func(x:Row)={
    ((x.getString(1),x.getString(2),x.getInt(3),x.getString(4),x.getString(5),x.getInt(6),x.getString(7),x.getString(8),x.getString(9),x.getString(10),
      x.getString(11),x.getString(12),x.getString(13),x.getString(14),x.getString(15),x.getString(16),x.getString(17),x.getString(18),x.getAs[Int]("date_hour_sk"),
      x.getAs[Int]("hour_num")),(x.getString(19),x.getString(20),x.getFloat(21),x.getInt(22)))
  }
  def func2(x: ((String, String, Int, String, String, Int, String, String, String, String, String, String, String, String, String, String, String, String,Int, Int),
    Iterable[(String, String, Float, Int)]),load_time:String,snapshot_date:String)= {
    val guestSessionIdList = mutable.MutableList[String]()
    val userIdList = mutable.MutableList[String]()
    val errSessionIdList = mutable.MutableList[String]()
    val termSessionIdList = mutable.MutableList[String]()
    var total_action_time:Float = 0
    for((user_id,session_id,t_action,errCode) <- x._2){
      if(user_id == "null") {guestSessionIdList += session_id} else {userIdList += user_id}
      if(errCode == 1){errSessionIdList += session_id}
      if(errCode == 0 && t_action == 0.0){termSessionIdList += session_id}
      total_action_time += t_action
    }
    Mapper(x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,x._1._8,x._1._9,x._1._10,x._1._11,x._1._12,x._1._13,x._1._14,x._1._15,x._1._16,x._1._17,x._1._18,x._1._19,
      guestSessionIdList.size,guestSessionIdList.toSet.size,userIdList.size,userIdList.toSet.size,errSessionIdList.size,errSessionIdList.toSet.size,
      termSessionIdList.size,termSessionIdList.toSet.size,total_action_time,x._1._20,load_time,snapshot_date)
  }
  case class Mapper(product_id:String,platform:String,star_level:Int,hierarchy:String,tag_name:String,tag_step:Int,tag_detail:String,tag_id:String,page_url:String,referer:String,
                    app_id:String,app_version:String,palcode:String,os_type:String,os_version:String,country:String,province:String,city:String,date_hour_sk:Int,
                    guest_pv:Int,guest_uv:Int,mpv:Int,muv:Int,err_pv:Int,err_uv:Int,terminate_pv:Int,terminate_uv:Int,total_action_time:Float,hour_num:Int,load_time:String,snapshot_date:String)

}
