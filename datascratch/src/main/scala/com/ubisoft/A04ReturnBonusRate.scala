package com.ubisoftubisoft

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object A04ReturnBonusRate {
  def main(args: Array[String]): Unit = {
    val yesterday = args(0).trim
    val date = args(0).replace("-","")
    val pid = args(1).trim
    /**
      * 建立SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("A04ReturnBonusRate Etl")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    import spark.implicits._
    val t_game_platform_dim = spark.sql("select distinct gametype,game,platform from ibds_dw.t_game_platform_dim")
    val df = spark.sql("select product_id,platform,gametype,split(billtime,' ')[0] as exec_date,12*hour(billtime) + cast(minute(billtime)/5 as int) + 1 as plot_time_label,sum(account) as monetary, sum(cus_account+account) as return from data_warehouse.logs_orders " +
      String.format("where dt='%s' and game_kind in ('5','8') and product_id='%s' group by product_id,platform,gametype,split(billtime,' ')[0],12*hour(billtime) + cast(minute(billtime)/5 as int) + 1",yesterday,pid))
    val df2 = df.rdd.map(x =>(x.getString(0)+"|"+x.getString(1)+"|"+x.getString(2)+"|"+x.getString(3),(x.getInt(4),x.getDouble(5).toFloat,x.getDouble(6).toFloat))).groupByKey().map(x =>func(x)).flatMap(_.split("\\^"))
      .map(x => (x.split("\\|")(0),x.split("\\|")(1),x.split("\\|")(2),x.split("\\|")(3),x.split("\\|")(4),x.split("\\|")(5),x.split("\\|")(6),x.split("\\|")(6).toFloat/x.split("\\|")(5).toFloat))
      .toDF("product_id","platform","gametype","exec_date","plot_time_label","monetary","return","rate")
    df2.createOrReplaceTempView("table")
    val df3 = df2.repartition(1).cache()
    df3.write.option("header",true).mode(SaveMode.Overwrite).csv("hdfs:///home/shelton/datashare/plotcsv_"+date)
    df3.write.mode(SaveMode.Overwrite).json("hdfs:///home/shelton/datashare/plotjson_"+date)
    df3.unpersist()
    val df4 = spark.sql("select product_id,platform,gametype,exec_date,max(rate) max,min(rate) min,stddev_samp(rate) std,avg(rate) avg,percentile(rate,0.5) median " +
      " from table group by " +
      "product_id,platform,gametype,exec_date").repartition(1).cache()
    //percentile(rate,0.25) Q1,percentile(rate,0.75) Q3,percentile(rate,0.95) P95,percentile(rate,0.99) P99
    df4.write.option("header",true).mode(SaveMode.Overwrite).csv("hdfs:///home/shelton/datashare/statscsv_"+date)
    df4.write.mode(SaveMode.Overwrite).json("hdfs:///home/shelton/datashare/statsjson_"+date)
    df4.unpersist()
    spark.close()
    println("###########finish#########")
  }
  def func(x: (String, Iterable[(Int, Float, Float)])) = {
    var res = ""
    def getSum(key:Int):String = {
      var a  = 0.0
      var b = 0.0
      x._2.foreach(tuple => {
        if(tuple._1 <= key){
          a += tuple._2
          b += tuple._3
        }
      })
      val res = key.toString + "|"+a.toString+"|"+b.toString
      res
    }
    x._2.foreach(tuple =>{
      res= res +"^"+ x._1+"|"+getSum(tuple._1)
    })
    res.substring(1)
  }

}
