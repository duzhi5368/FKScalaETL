package com.ubisoft.etl.app

import java.text.SimpleDateFormat
import java.util.Date

import com.ubisoft.caseclass.UsersLoginMapper
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.Map

object UsersLoginPlatformEtlApp {
  case  class UsersLoginPlatFormMapper(date_hour_fk:BigInt, user_fk:String,platform:String,load_time:String, dt:String)
  val product = List("A01","A02","A03","A04","A05","A06","B01","B04","B05","B06","B07","B08","B79","C01","C02","C04","C05","C07")
  def main(args: Array[String]): Unit = {
    val yesterday = args(0) //"2018-04-15"
    val start_full_date_hour = yesterday + "-00"
    val end_full_date_hour = yesterday +"-23"
    val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
    /**
      * 建立SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("Login Etl")
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
    val sql_select = String.format("select a.*,b.platform bplatform from (select * from ibds_staging.t_customers_login_items where  snapshot_date = '%s' and  split(login_time,'\\\\+')[0] >='%sT00:00:00.000' and " +
      "split(login_time,'\\\\+')[0] <='%sT23:59:59.999' and login_game != 'web' and login_game !='vipMember') a left join  ibds_dw.t_product_platform_dim b on a.login_game = regexp_replace(b.id_sk,'_','') ",yesterday,yesterday,yesterday)
    spark.sql(sql_select).rdd.map(a => {
      var platform = a.getAs[String]("bplatform")
      if(platform == null){platform = a.getAs[String]("login_game")}
      UsersLoginPlatFormMapper(BigInt(date_hour_sk_map(a.getString(7).split("T")(0) + "-" + a.getString(7).split("T")(1).split(":")(0)).toString), a.getString(3) + "_" + a.getString(4),
        platform,sysTime, a.getString(7).split("T")(0))
    }
    ).toDF().groupBy("date_hour_fk","user_fk","platform","load_time","dt").count().createOrReplaceTempView("temptable")
    val df = spark.sql("select date_hour_fk,user_fk,platform,count as login_count,load_time,dt  from  temptable")
    df.repartition(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_login_game_hourly_fact")
    spark.close()

  }

}
