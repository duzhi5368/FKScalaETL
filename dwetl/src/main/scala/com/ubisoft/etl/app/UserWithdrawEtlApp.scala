package com.ubisoft.etl.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.ubisoft.caseclass.{CustomerMapper, WithdrawMapper}

import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
object UserWithdrawEtlApp {
  val withdraw_count = 1
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
    val sql = String.format("select a.*,b.login_name  from (select *  from ibds_staging.t_credit_logs where snapshot_date ='%s'  and    split(created_date,'\\\\+')[0] >= '%sT00:00:00.000' and split(created_date,'\\\\+')[0] <= '%sT23:59:59.999') a " +
      "  left outer join    (select product_id,customer_id,login_name from ibds_staging.t_customers where snapshot_date = '%s') b " +
      " on a.product_id = b.product_id and a.customer_id = b.customer_id  ",yesterday,yesterday,yesterday,yesterday)
    val df = spark.sql(sql).rdd.filter(a =>
      a.getString(12).equals("111200") || a.getString(12).equals("111202") || a.getString(12).equals("111203") || a.getString(12).equals("111205")
    ).map(a => {
      val date_hour_sk_map_key = a.getString(3).split("T")(0) + "-" + a.getString(3).split("T")(1).split(":")(0)
      if (a.getString(12).equals("111200")) {
        WithdrawMapper(BigInt(date_hour_sk_map(date_hour_sk_map_key)),a.getString(0)+"_"+a.getString(33), currencyMap(a.getString(5)),
          withdraw_count, BigDecimal(a.getFloat(19)),a.getFloat(19),sysTime, a.getString(3).split("T")(0))
      }
      else {
        WithdrawMapper(BigInt(date_hour_sk_map(date_hour_sk_map_key)), a.getString(0) + "_" + a.getString(33), currencyMap(a.getString(5)),
          -withdraw_count, -BigDecimal(a.getFloat(19)),-a.getFloat(19),sysTime,a.getString(3).split("T")(0))
      }
    }).toDF()
    .groupBy("date_hour_fk", "user_fk","currency",  "load_time","dt")
    .agg(sum("withdraw_count").as("withdraw_count"),sum("amount").as("withdraw_amount"),
      max("amount_float").as("withdraw_max"),min("amount_float").as("withdraw_min")).rdd
      .map(func(_)).toDF().repartition(1)
      .write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_withdraw_hourly_fact")
    spark.close()
  }




  case class WithdrawResMapper(date_hour_fk:BigInt,user_fk:String,currency:Int,withdraw_count:Int,withdraw_amount:Float,withdraw_max:Float, withdraw_min:Float,
                               load_time:String,dt:String)
  def func(row:Row):WithdrawResMapper= {
    val date_hour_fk  = BigDecimal(row.getAs[java.math.BigDecimal]("date_hour_fk")).toBigInt()
    var withdraw_max = row.getAs[Float]("withdraw_max")
    if(withdraw_max<0) {withdraw_max = 0}
    var withdraw_min = row.getAs[Float]("withdraw_min")
    if(withdraw_min <0) {withdraw_min = 0}
    val currency = row.getAs[Int]("currency")
    val withdraw_count = row.getAs[Long]("withdraw_count").toInt
    WithdrawResMapper(date_hour_fk,row.getAs[String]("user_fk"),currency,withdraw_count,
      BigDecimal(row.getAs[java.math.BigDecimal]("withdraw_amount")).toFloat,withdraw_max,withdraw_min,row.getAs[String]("load_time"),row.getAs[String]("dt"))
  }
}
