package com.ubisoft.etl.app

import java.text.SimpleDateFormat
import java.util.Date

import com.ubisoft.caseclass.WithdrawMapper
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.Map

object UserXimaEtlApp{
  val xima_count = 1
  def main(args: Array[String]): Unit = {
    val yesterday = args(0)       //"2018-04-15"
    val start_full_date_hour = yesterday + "-00"
    val end_full_date_hour = yesterday +"-23"
    val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
    /**
      * 建立SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("withdraw Etl")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //获得当天所有的date_hour_sk
    val date_hour_sk_map:Map[String,Int] = Map()
    val sql_hour_sk = String.format("select  full_date_hour,date_hour_sk  from ibds_dw.t_date_hourly_dim where full_date_hour >='%s' and full_date_hour  <='%s' ",start_full_date_hour,end_full_date_hour)
    spark.sql(sql_hour_sk).rdd.collect().foreach(a =>date_hour_sk_map(a.getString(0)) = a.getInt(1))
    //获得所有的t_currency_dim映射关系
    val currencyMap:Map[String,Int] = Map()
    spark.sql("select currency_id,currency_code from ibds_dw.t_currency_dim").rdd.collect().foreach(a =>currencyMap(a.getString(1)) = a.getInt(0))
    //按小时处理
    val sql = String.format("select a.product_id,a.currency,a.trans_code,a.created_date,a.game_amount,b.login_name  from" +
      " (select *  from ibds_staging.t_credit_logs where snapshot_date ='%s'  and    split(created_date,'\\\\+')[0] >= '%sT00:00:00.000' and split(created_date,'\\\\+')[0] <= '%sT23:59:59.999') a " +
      "  left outer join    (select product_id,customer_id,login_name from ibds_staging.t_customers where snapshot_date = '%s') b " +
      " on a.product_id = b.product_id and a.customer_id = b.customer_id  ",yesterday,yesterday,yesterday,yesterday)
    val df = spark.sql(sql).rdd.filter(a =>
      a.getString(2).equals("112501")
    ).map(a => {
      val date_hour_sk_map_key = a.getString(3).split("T")(0) + "-" + a.getString(3).split("T")(1).split(":")(0)
      (BigInt(date_hour_sk_map(date_hour_sk_map_key)),a.getString(0)+"_"+a.getString(5), currencyMap(a.getString(1)),
        xima_count,a.getFloat(4),sysTime, a.getString(3).split("T")(0))
    }).toDF("date_hour_fk","user_fk","currency","xima_count","xima_amount","load_time","dt")
    .groupBy("date_hour_fk", "user_fk","currency",  "load_time","dt")
    .agg(sum("xima_count").as("xima_count"),sum("xima_amount").as("xima_amount"),
      max("xima_amount").as("xima_max"),min("xima_amount").as("xima_min"))
      .rdd.map(x =>(BigDecimal(x.getAs[java.math.BigDecimal]("date_hour_fk")).toBigInt(),x.getString(1),x.getInt(2),x.getLong(5).toInt,
    x.getAs[Double](6).toFloat,x.getFloat(7),x.getFloat(8),x.getString(3),x.getString(4))).toDF("date_hour_fk","user_fk",
    "currency","xima_count","xima_amount","xima_max","xima_min","load_time","dt")
    df.repartition(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_xima_hourly_fact")
    spark.close()
  }
}

