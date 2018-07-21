package com.ubisoft

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object T_Electronic_Games_MigrateApp {
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
    val today = dateFormat.format(new Date)
    val snapshot_date = getNDayAgo(today,1)
    val url ="jdbc:mysql://10.180.19.110:4315/wms?tinyInt1isBit=false"
    val user = "bigdata"
    val password = "Ae%ULnuy5RChs!}(+5+TT"
    val query = "(select GameId,GameProvider,ProductId,Display,EnglishName,ServerGameId,ChineseName,GameType,GameStyle,Paylines,PlayerType,Jackpot,Trial," +
      "Mobile,Picture1,Picture2,Picture3,Picture4,Picture1_extension,Likes,MaxWinMultiple,BetNumber,MaxPrize,PopularValue,Score,Star,Remarks,BonusUrl,IsNew," +
      "IsRecommend,ID,MarvelBonusUrl,IsSpecialty,Language,IsHot,IsPromotion,CONCAT(REPLACE(TRIM(DATE_FORMAT(createtime,'%Y-%m-%d %H:%i:%S')),' ','T'),'.000+08:00')  " +
      "as  createtime,IsShare, ShareType,IsFree,publish_state,CONCAT(REPLACE(TRIM(DATE_FORMAT(updateTime,'%Y-%m-%d %H:%i:%S')),' ','T'),'.000+08:00')  as  updateTime," +
      "CONCAT(REPLACE(TRIM(DATE_FORMAT(publishTime,'%Y-%m-%d %H:%i:%S')),' ','T'),'.000+08:00')  as  publishTime ," +
      String.format("'%s' as snapshot_date from t_electronic_games)  t_electronic_games",snapshot_date)
    println("*************sql******************")
    println(query)
    /**
      * 建立spark Session
      */
    val spark = SparkSession.builder()
      .appName("t_electronic_games migrate")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val mysqlDF:DataFrame = spark.read.format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",query)
      .option("driver","com.mysql.jdbc.Driver")
      .load()
    spark.sql("use ibds_staging")
    mysqlDF.repartition(1).write.mode(SaveMode.Overwrite).insertInto("t_electronic_games")
    println("####################END############################")


  }

}
