package com.ubisoft.knightfunnel

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MarketClickstreamTag {
  def main(args: Array[String]): Unit = {
    val dt = args(0).trim
    val pID = args(1).trim
    val year = dt.split("-")(0)
    val month = dt.split("-")(1)
    val day =dt.split("-")(2)
  /**
    * 建立sparksession
    */
    val spark = SparkSession.builder()
      .appName("clickstream_tag")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    import spark.implicits._
    val msg = init(pID,dt)
    val config_dict = spark.sparkContext.textFile(msg).map(a =>
      Tuple2(a.split("=")(0),a.split("=")(1))
    ).flatMapValues(_.split(",")).toDF("tag_name","tag").distinct()
    val dosql_tag = String.format("select click_time,tag_id,session_id,user_id,p_id,game_type_sk from IBDS_staging.t_clickstream where dt='%s' and p_id='%s'",dt,pID)
    println(dosql_tag)
    val df = spark.sql(dosql_tag).rdd.map(x =>
    Tuple2(x.getAs[String](4) +"|"+x.getAs[String](1)+"|"+x.getAs[String](0).split("T")(0)+"|"+x.getAs[String](5),Tuple2(x.getAs[String](2),x.getAs[String](3))))
      .groupByKey().map(tagFunc)
      .toDF("time","tag","product_id","num_visitors","num_unique_visitors","num_members","num_unique_members","num_unique_sessions_for_members","game_type")
    df.join(config_dict,df("tag") === config_dict("tag"),"full_outer").selectExpr("time","tag_name","num_visitors","num_unique_visitors"
      ,"num_members","num_unique_members","num_unique_sessions_for_members","product_id","game_type").na.fill(Map("time"->dt,"tag_name" ->"其他",
      "num_visitors" ->0,"num_unique_visitors"->0,"num_members"->0,"num_unique_members"->0,"num_unique_sessions_for_members"->0,"product_id"->pID,"game_type"->"0"
    )).rdd.map(x =>
        Tuple2(x.getAs[String](0)+"|"+x.getString(1)+"|"+x.getString(7)+"|"+x.getAs[String](8),Array(x.getAs[Int](2),x.getAs[Int](3),x.getAs[Int](4),x.getAs[Int](5),x.getAs[Int](6)))
      ).groupByKey().map(resFunc(_,dt))
      .toDF("tag_name","tag_step","tag_detail","visitors_count","session_count","member_count","member_unique_count","unique_session_member_count","game_type","product_id","full_date")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dm_c01.t_c01_user_funnel_chart_fact")
    df.unpersist()
    spark.close()
    println("over over over over")
  }

  def resFunc(x:Tuple2[String,Iterable[Array[Int]]],dt:String) = {
    var v0,v1,v2,v3,v4 = 0
    for(y <- x._2){
      v0 += y(0)
      v1 += y(1)
      v2 += y(2)
      v3 += y(3)
      v4 += y(4)
    }
    val time = x._1.split("\\|")(0)
    val tag_name = BaseEncoding.base64().encode(x._1.split("\\|")(1).split("_")(0).getBytes(Charsets.UTF_8))
    var tag_step = 0
    var tag_detail = ""
    try {
      tag_step = x._1.split("\\|")(1).split("_")(1).replace("step","").trim.toInt
    }catch {case e:Exception =>{}}
    try{
      tag_detail = BaseEncoding.base64().encode(x._1.split("\\|")(1).split("_")(2).getBytes(Charsets.UTF_8))
    }catch {case e:Exception =>{}}
    val num_visitors = v0
    val num_unique_visitors = v1
    val num_members = v2
    val num_unique_members = v3
    val num_unique_sessions_for_members = v4
    val game_type = x._1.split("\\|")(3)
    val product_id  = x._1.split("\\|")(2)
    Tuple11(tag_name,tag_step,tag_detail,num_visitors,num_unique_visitors,num_members,num_unique_members,num_unique_sessions_for_members,game_type,product_id,dt)
  }


  def tagFunc(x:Tuple2[String,Iterable[Tuple2[String,String]]]) = {
    val time:String = (x._1).split("\\|")(2)
    val product_id:String = x._1.split("\\|")(0)
    val tag:String = x._1.split("\\|")(1)
    val features = x._2

    val num_event = features.size
    val members_id:ArrayBuffer[String]= ArrayBuffer.empty
    val members_session:ArrayBuffer[String] = ArrayBuffer.empty
    val visitors:ArrayBuffer[String] = ArrayBuffer.empty
    for (item <- features){
      if(item._2 == ""){
        visitors += item._1
      }else{
        members_session += item._1
        members_id += item._2
      }
    }
    val num_visitors = visitors.length
    val num_unique_visitors = visitors.toSet.size
    val num_members = members_id.size
    val num_unique_members = members_id.toSet.size
    val num_unique_sessions_for_members = members_session.toSet.size
    val game_type = x._1.split("\\|")(3)
    Tuple9(time,tag,product_id,num_visitors,num_unique_visitors,num_members,num_unique_members,num_unique_sessions_for_members,game_type)
  }

  def init(pid: String, dt: String):String  = {
    val product_version_cfg:ArrayBuffer[String] = mutable.ArrayBuffer.empty
    val strDate = dt.replace("-","")
    val base_path = s"/apps/cronJobs/marketClickStream/conf/$pid"
    val inputPath = new Path(base_path)
    val fs:FileSystem = inputPath.getFileSystem(new Configuration())
    val statuses = fs.listStatus(inputPath)
    for (s<-statuses){
      if(s.isDirectory){
        val fileName = s.getPath.getName
        val err_msg = s"OMG! %{fileName} is a directory!"
        return err_msg
      }else if(s.isFile){
        product_version_cfg += s.getPath.getName // 获得的是相对路径名称
      }
    }
    var res = "0"
    for(version <-product_version_cfg){
      if(version.toInt > strDate.toInt || res.toInt >version.toInt){
      }else{
        res = version
      }
    }
    return base_path+"/"+res
  }
}
