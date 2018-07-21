package com.ubisoft

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object T_ElectronicGamesApp {
  def main(args: Array[String]): Unit = {
    val yesterday = args(0) //暨snapshot_date
    val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
    /**
      * 建立SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("T_Electronic_Games Etl")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val sql = String.format("select gametype,product_id,platform,loginname,account,valid_account,cus_account from ibds_staging.t_orders where " +
      " snapshot_date = '%s' and game_kind in (5,8)",yesterday)
    val df = spark.sql(sql).rdd.cache()
    val df1 = df.map(a => Tuple2(a.getAs[String](0)+"|"+a.getAs[String](1)+"|"+a.getAs[String](2),Tuple4(a.getAs[String](3),BigDecimal(a.getAs[Float](4)),
      BigDecimal(a.getAs[Float](5)),BigDecimal(a.getAs[Float](6))))).groupByKey().map(x =>fun(x,sysTime,yesterday)).toDF("gametype","product_id","platform","member_cnt","monetary"
      ,"valid_monetary","return_rate","return_rate_median","return_max","return_min","bet_max","bet_avg","monetary_median","cnt_median","load_time","snapshot_date")

    df.map(a => Tuple2(a.getAs[String](0)+"|"+a.getAs[String](1)+"|"+a.getAs[String](2)+"^"+a.getAs[String](3),Tuple3(BigDecimal(a.getAs[Float](4)),
      BigDecimal(a.getAs[Float](5)),BigDecimal(a.getAs[Float](6))))).groupByKey().map(fun2).filter(x =>x._5 != None)
      .toDF("gametype","product_id","platform","loginname","return_rate","monetary_total","cnt_total").createOrReplaceTempView("temptable")

    val df2 = spark.sql("select gametype,product_id,platform,mean(return_rate) return_rate_mean,stddev(return_rate) return_rate_std,max(return_rate) return_rate_max,min(return_rate) return_rate_min " +
      " ,avg(monetary_total) monetary_mean ,stddev(monetary_total)  monetary_std,avg(cnt_total) cnt_mean,stddev(cnt_total) cnt_std from temptable " +
      " group by gametype,product_id,platform ")

    val t_game_platform_dim = spark.sql("select id_sk game_platform_sk,gametype,platform from ibds_dw.t_game_platform_dim")
    val t_product_platform_dim = spark.sql("select id_sk  product_platform_sk,product_id,platform from ibds_dw.t_product_platform_dim")
//    val t_electronic_games = spark.sql(String.format("select gameid,productid,gameprovider from ibds_staging.t_electronic_games where snapshot_date = '%s'",yesterday))
    val df3= df1.join(df2,df1("gametype") === df2("gametype") && df1("product_id") === df2("product_id") && df1("platform") === df2("platform"),"left_outer")
      .drop(df2("gametype")).drop(df2("product_id")).drop(df2("platform"))
    val df4 = df3.join(t_game_platform_dim,df3("gametype") === t_game_platform_dim("gametype") && df3("platform") === t_game_platform_dim("platform"),"left_outer")
      .drop(t_game_platform_dim("gametype")).drop(t_game_platform_dim("platform"))
    val df5 = df4.join(t_product_platform_dim,df4("product_id") === t_product_platform_dim("product_id") && df4("platform") === t_product_platform_dim("platform"),"left_outer")
      .drop(t_product_platform_dim("product_id")).drop(t_product_platform_dim("platform"))
//    val df6 = df5.join(t_electronic_games,df5("gametype") === t_electronic_games("gameid") && df5("product_id") === t_electronic_games("productid") && df5("platform") === t_electronic_games("gameprovider"),"full_outer")
    df5.select("game_platform_sk","product_platform_sk","member_cnt","monetary","valid_monetary","return_rate","return_rate_mean","return_rate_median","return_rate_std","return_rate_max",
      "return_rate_min","return_max","return_min","bet_max","bet_avg","monetary_mean","monetary_std","monetary_median","cnt_mean","cnt_median","cnt_std","load_time","snapshot_date")
      .repartition(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_game_electronic_daily_fact")
    df.unpersist()
    println("finish------------------------------------finish")
  }

  def fun(row: (String, Iterable[(String, BigDecimal, BigDecimal, BigDecimal)]),load_time:String,snapshot_date:String) = {
    val user_member:mutable.Set[String] = mutable.Set.empty
    val account_list:ArrayBuffer[BigDecimal] = ArrayBuffer.empty
    val valid_account_list:ArrayBuffer[BigDecimal] = ArrayBuffer.empty
    val cus_account_list:ArrayBuffer[BigDecimal] = ArrayBuffer.empty
    for(tuple <- row._2){
      user_member += tuple._1
      account_list += tuple._2
      valid_account_list += tuple._3
      cus_account_list += tuple._4
    }
    //get member_cnt,monetary,valid_monetary
    val member_cnt:Int =user_member.size
    val monetary = account_list.sum
    val valid_monetary = valid_account_list.sum
    //get return_rate
    val profit = -cus_account_list.sum
    var return_rate:Option[Float] =None
    if(valid_monetary != 0){
      return_rate = Option((profit/valid_monetary).toFloat)
    }
    //get temp
    val temp_array = row._2.groupBy(x => x._1).map(x =>fun3(x)).filter(_._1 != None)

    //define three median
    var return_rate_median:Option[Float] = None
    var monetary_median:Option[Float] = None
    var cnt_median:Option[Float] = None
    if(temp_array.size >0){
      //get return_rate_median
      val return_rate_ordered = temp_array.map(_._1).toArray.sortBy(x =>x)
      var len = return_rate_ordered.length
      println(return_rate_ordered(0))
      if(len %2 == 1){return_rate_median = return_rate_ordered(len/2)} else {return_rate_median = Option((return_rate_ordered(len/2).get + return_rate_ordered(len/2-1).get)/2.0f)}
      //get monetary_median
      val monetary_ordered = temp_array.map(_._2).toArray.sortBy(x =>x)
      len = monetary_ordered.length
      if(len %2 == 1){monetary_median = Option(monetary_ordered(len/2).toFloat)} else {monetary_median = Option((monetary_ordered(len/2) + monetary_ordered(len/2-1)).toFloat/2.0f)}
      //get cnt_median
      val cnt_ordered = temp_array.map(_._3).toArray.sortBy(x =>x)
      len = cnt_ordered.length
      if(len %2 == 1){cnt_median = Option(cnt_ordered(len/2).toFloat)} else {cnt_median = Option((cnt_ordered(len/2) + cnt_ordered(len/2-1))/2.0f)}
    }
    //get return_max and return_min
    val return_max = cus_account_list.max
    val return_min = cus_account_list.min
    //get bet_max,bet_avg
    val bet_max = account_list.max
    val bet_avg = account_list.sum/BigDecimal(account_list.size)
    //get result
    var gametype =""
    var product_id =""
    var platform =""
    try{
      gametype = row._1.split("\\|")(0)
    }catch {case e:Exception => {}}
    try{
      product_id = row._1.split("\\|")(1)
    }catch {case e:Exception => {}}
    try{
      platform = row._1.split("\\|")(2)
    }catch {case e:Exception => {}}
    (gametype,product_id,platform,member_cnt,monetary,valid_monetary,return_rate,return_rate_median,return_max,return_min,bet_max,bet_avg,monetary_median,cnt_median,load_time,snapshot_date)
  }

  def fun2(row: (String, Iterable[(BigDecimal, BigDecimal, BigDecimal)]))= {
    val account_list:ArrayBuffer[BigDecimal] = ArrayBuffer.empty
    val valid_account_list:ArrayBuffer[BigDecimal] = ArrayBuffer.empty
    val cus_account_list:ArrayBuffer[BigDecimal] = ArrayBuffer.empty
    for(tuple <- row._2){
      account_list += tuple._1
      valid_account_list += tuple._2
      cus_account_list += tuple._3
    }
    //get return_rate
    val valid_monetary = valid_account_list.sum
    val profit = -cus_account_list.sum
    var return_rate:Option[Float] = None
    if(valid_monetary != 0){
      return_rate = Option((profit/valid_monetary).toFloat)
    }
    //get monetary_total
    val monetary_total = account_list.sum
    //get cnt_total
    val cnt_total = account_list.size
    //get key
    val gametype_pid_platform = row._1.split("\\^")(0)
    var loginname =""
    try{      loginname = row._1.split("\\^")(1)     } catch {case e:Exception =>{}}
    var gametype =""
    var product_id =""
    var platform =""
    try{
      gametype = gametype_pid_platform.split("\\|")(0)
    }catch {case e:Exception => {}}
    try{
      product_id = gametype_pid_platform.split("\\|")(1)
    }catch {case e:Exception => {}}
    try{
      platform = gametype_pid_platform.split("\\|")(2)
    }catch {case e:Exception => {}}
    (gametype,product_id,platform,loginname,return_rate,monetary_total.toFloat,cnt_total.toFloat)
  }

  def fun3(row: (String, Iterable[(String, BigDecimal, BigDecimal, BigDecimal)]))= {
    val user_id = row._1
    var monetary:BigDecimal = 0
    var valid_monetary:BigDecimal = 0
    var profit:BigDecimal = 0
    for(tuple <- row._2){
      monetary += tuple._2
      valid_monetary += tuple._3
      profit -= tuple._4
    }
    var return_rate:Option[Float]= None
    if(valid_monetary != 0){
      return_rate = Option((profit/valid_monetary).toFloat)
    }
    (return_rate,monetary,row._2.size)
  }
}
