package com.ubisoft.etl.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.ubisoft.caseclass.{CustomerMapper, DepositMapper}

import scala.collection.mutable.Map
import org.apache.spark.sql.functions._
object UserDepositEtlApp {
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
      .appName("Deposit Etl")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //获得当天所有的date_hour_sk
    val date_hour_sk_map:Map[String,Int] = Map()
    val sql_hour_sk = String.format("select  full_date_hour,date_hour_sk  from ibds_dw.t_date_hourly_dim where full_date_hour >='%s' and full_date_hour  <='%s' ",start_full_date_hour,end_full_date_hour)
    spark.sql(sql_hour_sk).rdd.collect().foreach(a =>date_hour_sk_map(a.getString(0)) = a.getInt(1))
//    date_hour_sk_map.foreach(println)
    //获得所有的t_currency_dim映射关系
    val currencyMap:Map[String,Int] = Map()
    spark.sql("select currency_id,currency_code from ibds_dw.t_currency_dim").rdd.collect().foreach(a =>currencyMap(a.getString(1)) = a.getInt(0))
//    currencyMap.foreach(println)
    //开始按小时处理
    val sql_select = String.format("select a.*,b.login_name  from (select *  from ibds_staging.t_credit_logs where snapshot_date ='%s'  and   split(created_date,'\\\\+')[0] >= '%sT00:00:00.000' and  split(created_date,'\\\\+')[0]  <= '%sT23:59:59.999') a " +
      "  left outer join    (select product_id,customer_id,login_name from ibds_staging.t_customers where snapshot_date = '%s') b " +
      " on a.product_id = b.product_id and a.customer_id = b.customer_id  ",yesterday,yesterday,yesterday,yesterday)
    spark.sql(sql_select)
        .rdd.filter(a =>
      a.getString(12).equals("111101") || a.getString(12).equals("111105") || a.getString(12).equals("111106")
    ).map(func(_,sysTime,date_hour_sk_map,currencyMap)).toDF()
    .groupBy("date_hour_fk", "user_fk", "deposit_channel_fk","currency","load_time","dt")
      .agg(sum("deposit_count").as("deposit_count"),sum("deposit_amount").cast("float").as("deposit_amount"))
      .selectExpr("date_hour_fk","user_fk","deposit_channel_fk","currency","deposit_count","deposit_amount","load_time","dt").repartition(1)
      .write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_deposit_hourly_fact")
    spark.close()
  }
  /**
    * 数组转换函数
    * @param x
    * @return
    */
    def func(x:Row,sysTime:String,date_hour_sk_map:Map[String,Int],currencyMap:Map[String,Int]):DepositMapper = {
      val y  = new Array[String](8)
      val date_hour_sk_map_key = x.getString(3).split("T")(0) + "-" + x.getString(3).split("T")(1).split(":")(0)
      y(0) =date_hour_sk_map(date_hour_sk_map_key).toString
      y(1) = x.getString(0)+"_"+x.getString(33)
      if(x.getString(12).equals("111101")){
        y(2) = "1"
      }
      if(x.getString(12).equals("111106")){
        y(2) = "2"
      }
      if(x.getString(12).equals("111105")){
        if(x.getString(25).equals("bitcoin") || x.getString(25).equals("cubitpay")){
          y(2) = "3"
        }else if(x.getString(25).equals("card")){
          y(2) = "4"
        }else if(x.getString(25).equals("mobile")){
          y(2)  = "5"
        }else if(x.getString(25).equals("qrcode")){
          y(2) = "6"
        }else if(x.getString(25).equals("pay")){
          y(2) = "7"
        }else{y(2) = "8"}
      }
      y(3) =currencyMap(x.getString(5)).toString
      y(4) = "1"
      y(5) = x.getFloat(19).toString
      y(6) = x.getString(3).split("T")(0)
      DepositMapper(y(0).trim.toLong, y(1), y(2).trim.toInt, y(3).trim.toInt, y(4).trim.toInt, BigDecimal(y(5).trim),sysTime, y(6))
  }


}
