package com.ubisoft

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
object GetA02ContinueWin {
  def main(args: Array[String]): Unit = {
    /**
      * 建立SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("Bank Etl")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    import spark.implicits._
    val writer = new PrintWriter(new File("natasha.csv"))
    writer.write("日期,连赢次数,用户ID\n")
    /**
      *执行统计
      */
    val sql = "select dt,user_id,    cus_account,valid_account,account,playtype,gmcode,billtime from data_warehouse.logs_orders where " +
      " product_id = 'A02' and platform = 'AGQJ' and gametype = 'BAC' and game_kind = 3 and dt >= '2018-04-01' and dt <= '2018-04-30' and playtype in (1,2) "
    spark.sql(sql).rdd.map(x => Tuple2(x.getAs[String]("dt") + "|"+x.getAs[String]("user_id"),
    Tuple6(x.getFloat(2),x.getFloat(3),x.getFloat(4),x.getAs[Int](5),x.getString(6),x.getString(7))))
    .groupByKey().map(func(_)).filter(_._2 >= 5).collect().foreach(x =>{
//      println(x)
      for(i <- 5 to x._2 ){
        writer.write(x._1+","+i+","+x._3+"\n")
      }
    })
    writer.close()
  }
  def func(tuple: (String, Iterable[(Float, Float, Float, Int, String, String)])) = {
    //获得对冲的局号
    val  offser_gmcode = ArrayBuffer[String]()
    tuple._2.map(x =>Tuple2(x._5,x._4)).groupBy(x => x._1).foreach(x =>{
      if(x._2.toList.contains(Tuple2(x._1,1))  &&  x._2.toList.contains(Tuple2(x._1,2))){
        offser_gmcode += x._1
      }
    })
    //统计分析
    val dt = tuple._1.split("\\|")(0)
    val user_id = tuple._1.split("\\|")(1)
    val records = tuple._2.toList.sortBy(_._6)
    val continueWinTimes = ArrayBuffer[Int]()
    var i = 0
    for(record <- records){
      breakable{
        val cus_account = record._1
        //        val valid_account = record._2
        val account = record._3
        val gmcode = record._5
        if(cus_account<0 ||offser_gmcode.contains(gmcode) ){
          continueWinTimes.+=(i);i = 0;break
        }
        if(account < 200 || cus_account ==0){
          break()
        }
        i += 1
      }
    }

    continueWinTimes.+=(i)
    var maxContinueWinTimes = continueWinTimes.max
    if(maxContinueWinTimes >=18) {
      maxContinueWinTimes = 18
    }
    Tuple3(dt,maxContinueWinTimes,user_id)
  }

}
