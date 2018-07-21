package com.ubisoft.etl.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.ubisoft.caseclass.UsersLoginDeviceMapper

import scala.collection.mutable.Map

object UsersLoginDeviceEtlApp {
  val login_device_list = Array("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17")
  def main(args: Array[String]): Unit ={
    val yesterday = args(0) //"2018-04-15"
    val start_full_date_hour = yesterday + "-00"
    val end_full_date_hour = yesterday +"-23"
    val sysTime =   new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
    /**
      * 建立SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("LoginDevice Etl")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //获得当天所有的date_hour_sk
    val date_hour_sk_map:Map[String,Int] = Map()
    val sql_hour_sk = String.format("select  full_date_hour,date_hour_sk  from ibds_dw.t_date_hourly_dim where full_date_hour >='%s' and full_date_hour  <='%s' ",start_full_date_hour,end_full_date_hour)
    spark.sql(sql_hour_sk).rdd.collect().foreach(a =>date_hour_sk_map(a.getString(0)) = a.getInt(1))
    /**
      * 按小时处理
      */
    val sql_select = String.format("select * from ibds_staging.t_customers_login_items where  snapshot_date = '%s' and   split(login_time,'\\\\+')[0]  >='%sT00:00:00.000' and split(login_time,'\\\\+')[0] <='%sT23:59:59.999' and login_game = 'web'",yesterday,yesterday,yesterday)
    spark.sql(sql_select)
       .rdd.map(fun(_,date_hour_sk_map)).map(a =>
      UsersLoginDeviceMapper(BigInt(a(0)),a(1),a(2),sysTime,a(3))
    ).toDF().groupBy("date_hour_fk","user_fk","login_device","load_time","dt").count().createOrReplaceTempView("temptable")
    spark.sql("select date_hour_fk,user_fk,login_device,count as login_count,load_time,dt from temptable").repartition(1)
    .write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_login_device_hourly_fact")

    spark.close()
  }
  /**
    *
    * @param a
    * @return
    */
  def fun(a: Row,date_hour_sk_map:Map[String,Int]):Array[String] = {
    val newArray = new Array[String](4)
    newArray(0) = date_hour_sk_map(a.getString(7).split("T")(0) +"-"+ a.getString(7).split("T")(1).split(":")(0)).toString
    newArray(1) = a.getString(3) + "_"+a.getString(4)
    newArray(2) = "-1"
    if(login_device_list.contains(a.getString(9))){
      newArray(2)  =a.getString(9)
    }
    newArray(3) = a.getString(7).split("T")(0)
    newArray
  }

}
