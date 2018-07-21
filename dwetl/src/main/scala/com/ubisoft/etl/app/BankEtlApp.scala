package com.ubisoft.etl.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.ubisoft.caseclass._

import scala.util.control.Breaks._
import scala.collection.mutable._
object BankEtlApp {
  /**
    * 中国的省份
    */
  val provinceList  = List("河北", "山西", "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东", "海南",
    "四川", "贵州", "云南", "陕西", "甘肃", "青海", "台湾", "内蒙古", "广西", "西藏", "宁夏", "新疆", "北京", "天津", "上海", "重庆",
    "香港", "澳门")
  val countryList=List("中国","台湾","越南")
  val municipality = List("北京", "天津", "上海", "重庆","香港", "澳门")
  val bB = List("招", "工", "农", "建", "交", "民", "光", "兴", "浦", "广", "圳", "平", "邮", "夏", "中信", "信", "中国", "比",
  "BTC", "btc", "CMB", "ABC", "CCB", "BOCOM", "渤海银行", "ICBC")
  val B = List("招商银行", "工商银行", "农业银行", "建设银行", "交通银行", "民生银行", "光大银行", "兴业银行", "上海浦东发展银行", "广东发展银行", "深圳发展银行", "平安银行",
  "中国邮政储蓄银行", "华夏银行", "中信银行", "农村信用社", "中国银行", "比特币", "比特币", "比特币", "招商银行", "农业银行", "建设银行", "交通银行",
  "渤海银行", "工商银行")


  /**
    * 主程序入口
    * @param args
    */
  def main(args: Array[String]): Unit = {
      val yesterday = args(0) //"2018-04-15"
      val sysTime =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'").format(new Date)
      println("系统时间："+sysTime)
      println("当前执行日期："+yesterday)
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
    /**
      * 获得省市区的表
      */
    import scala.collection.mutable.Map
    val city_province:Map[String,String] = Map()
    spark.sql("select city,province from ibds_dw.t_city_province_dim").rdd.map(row =>
    Tuple2(row.getString(0),row.getString(1))).collect().foreach(t =>city_province(t._1) = t._2)
    /**
      * bank
      */
    val sql = String.format("select a.*,b.login_name as login_name  from (select * from ibds_staging.t_customer_banks where snapshot_date = '%s') a " +
      "  left outer join    (select product_id,customer_id,login_name from ibds_staging.t_customers where snapshot_date = '%s') b " +
      " on a.product_id = b.product_id and a.customer_id = b.customer_id  ",yesterday,yesterday)
    println(sql)
    spark.sql(sql).rdd
      .map(func(_,sysTime,city_province))
      .toDF()
      .filter("priority_order = 1").selectExpr("user_fk","product_id","bank",
      "country","province","area","branch","priority_order","load_time","dt").repartition(1).write.mode(SaveMode.Overwrite).insertInto("ibds_dw.t_users_banks_fact")
    spark.close()
    println("success success success success")
  }

  /**
    * 处理函数
    * @param row
    * @return
    */
  def func(row: Row,sysTime:String,city_province:Map[String,String]): BankMapper = {
    val attributes: ArrayBuffer[String] = ArrayBuffer(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),
      row.getInt(5).toString,row.getInt(6).toString,row.getString(7),row.getString(8),row.getString(9),row.getString(10))
    /**
      * 增加area字段
      */
    attributes += attributes(2)
    /**
      * 获得系统时间
      */
    attributes += sysTime
    /**
      * country是中国的省份，且省份不是台湾，更正为CHINA
      */
    if(attributes(1).contains(countryList(1))){
      attributes(1) =countryList(1)
    }else{
      breakable{
        for(province <- provinceList){
          if(attributes(1).contains(province)){
            attributes(1) =countryList(0)
            break()
          }
        }
      }
    }
    if(attributes(1).contains("CHINA")){
      attributes(1) = countryList(0)
    }
    /**
      * 如果国家为越南，则将省份更改为越南地区
      */
    if(attributes(1).contains(countryList(2))){
      attributes(2) = "越南地区"
    }
    /**
      *产品'C07'为越南用户
      */
    if(attributes(0).equals("C07")){
      attributes(1) = countryList(2)
      attributes(2) = "越南地区"
    }
    /**
      * 获得省份
      */
//    println(attributes(0)+"_"+attributes(4)+"testtest")
    breakable{
      for(province <- provinceList){
        if(attributes(2).contains(province)){
          attributes(2) = province
          break()
        }
      }
    }
    //根据城市获得省份
    breakable{
      for(city <- city_province.keys){
        if(attributes(2).contains(city)){
          attributes(2) = city_province(city)
          break()
        }
      }
    }
    /**
      * 处理逗号分隔的格式问题
      * 包含两种1.“省份城市，省份城市”2.“省份，城市”
      */
    if(attributes(11).contains(",")){
      attributes(11) = attributes(11).split(",",2)(1)
    }else if(attributes(11).contains("，")){
      attributes(11) = attributes(11).split("，",2)(1)
    }
    /**
      * 获得area（城市），直辖市省份与城市相同
      */
    if(municipality.contains(attributes(2))){
      attributes(11)=attributes(2)
    }
    breakable{
      for(city <- city_province.keys){
        if(attributes(11).contains(city)){
          attributes(11) = city
          break()
        }
      }
    }
    /**
      * 清洗area错误的问题
      */
    attributes(11) = attributes(11).replace("其他城市:", "").replace("其他城市","").replace("省", "")
      .replace("市", "").replace(":", "").replace(",","").replace("：","").replace(" ","").replace("，","")

    /**
      * 统一bank名称
      */
    var bankname = attributes(3)
    breakable{
      for(i <- 0 until bB.length){
        val keyword = bB(i)
        if(bankname.contains(keyword)){
          bankname = B(i)
          break()
        }
      }
    }
    BankMapper(attributes(0)+"_"+attributes(10),attributes(0),bankname,attributes(1),attributes(2),attributes(11),attributes(7),attributes(6).toInt,attributes(12),attributes(9))
  }
}
