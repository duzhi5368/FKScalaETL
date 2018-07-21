package com.ubisoft.a04.monitor

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import java.io.{ByteArrayInputStream, InputStream}
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object T_MonitoringGames {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'")
  private val url = "/share/etl_staging/ip_city/GeoLite2-City.mmdb"
  private val is: InputStream = new ByteArrayInputStream(readHadoopFile(url))
  private val geoIPResolver = new DatabaseReader.Builder(is).withCache(new CHMCache).build()
  def main(args: Array[String]): Unit = {
    val snapshot_date  = args(0).trim
    val load_time = dateFormat.format(new Date)
    val spark = SparkSession.builder()
      .appName("T_MonitoringGames")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    /**
      * 获得配置文档
      */
    val confFile = "/share/monitor_game_config/stepconfig.conf"
    val confMap = mutable.Map[String,String]()
    spark.sparkContext.textFile(confFile).map(x =>(x.split("=")(0),x.split("=")(1))).collect().foreach(x =>confMap(x._1) = x._2)
    /**
      * 开始spark任务
      */
    val df = spark.sql("select agent,app_id,app_version,data,depth,device_brand,device_type,event_time,ip,isp,mac,network,os_type,os_version,page_url,palcode,product_id," +
      String.format("referer,session_id,tag_id,user_id,snapshot_date from test.t_monitor_games_staging  where snapshot_date  = '%s' ",snapshot_date))
    df.rdd.map(x =>func(x,load_time,confMap)).groupByKey().map(x =>func2(x)).flatMapValues(x =>x).map(x =>x._2).toDF().write.mode(SaveMode.Overwrite).insertInto("test.t_monitoring_games")
    spark.close()
  }
  def func(x: Row,load_time:String,confMap:mutable.Map[String,String]) = {
    val data = x.getString(3)
    val jsonParser = new JSONParser()
    val jsonObj: JSONObject = jsonParser.parse(data).asInstanceOf[JSONObject]
    val product_id = x.getAs[String]("product_id")
    val platform = jsonObj.get("platform").toString
    val session_id = x.getAs[String]("session_id")
    val procedure_id = jsonObj.get("procedure_id").toString
    val user_id = x.getAs[String]("user_id")
    val star_level = jsonObj.get("customer_level").toString.toInt
    val hierarchy = jsonObj.get("hierarchy").toString
    val tag_id = x.getAs[String]("tag_id")
    val page_url = x.getAs[String]("page_url")
    val referer = x.getAs[String]("referer")
    var tag_name = "未知"
    var tag_step = -1
    var tag_detail = "未知"
    try{
      val value = confMap(tag_id+","+hierarchy+","+referer)
      tag_name = value.split(",")(0)
      tag_step = value.split(",")(1).toInt
      tag_detail = value.split(",")(2)
    }catch {case e:Exception => {}}
    val click_time = x.getAs[String]("event_time")
    val app_id = x.getAs[String]("app_id")
    val app_version = x.getAs[String]("app_version")
    val device_type = x.getAs[String]("device_type")
    val device_brand = x.getAs[String]("device_brand")
    val os_type = x.getAs[String]("os_type") //可能没有
    val os_version = x.getAs[String]("os_version")
    val palcode = x.getAs[String]("palcode")
    val isp = x.getAs[String]("isp")
    val mac = x.getAs[String]("mac")
    val network = x.getAs[String]("network")
    val ip = x.getAs[String]("ip")
    val array = ip_analysis(ip)
    val country = array(0)
    val province = array(1)
    val city = array(2)
    var server_ip = ip
    if(jsonObj.get("server_ip") != null){server_ip = jsonObj.get("server_ip").toString}
    val server_array = ip_analysis(server_ip)
    val server_country = server_array(0)
    val server_province = server_array(1)
    val server_city = server_array(2)
    val errCode = jsonObj.get("errCode").toString.toInt
    val snapshot_date = x.getAs[String]("snapshot_date")
    ((session_id,procedure_id,tag_name),Mapper(product_id,platform,session_id,procedure_id,user_id,star_level,hierarchy,tag_id,page_url,referer,tag_name,tag_step,tag_detail,click_time,0.0,app_id,app_version,
    device_type,device_brand,os_type,os_version,palcode,isp,mac,network,ip,country,province,city,server_ip,server_country,server_province,server_city,errCode,load_time,snapshot_date))
  }
  case class Mapper(product_id:String,platform:String,session_id:String,procedure_id:String,user_id:String,star_level:Int,hierarchy:String,tag_id:String,page_url:String,referer:String,tag_name:String,tag_step:Int,
                    tag_detail:String,click_time:String,var t_action:Double,app_id:String,app_version:String,device_type:String,device_brand:String,os_type:String,os_version:String,palcode:String,
                    isp:String,mac:String,network:String,ip:String,country:String,province:String,city:String,server_ip:String,server_country:String,server_province:String,
                    server_city:String,errCode:Int,load_time:String,snapshot_date:String)
  def func2(x: ((String, String,String), Iterable[Mapper])) = {
    val rows = x._2.toList.sortBy(_.tag_step)
    for(i <- 0 until rows.length){
      try {
        rows(i).t_action = (dateFormat.parse(rows(i+1).click_time).getTime - dateFormat.parse(rows(i).click_time).getTime)/1000.0
      }catch {
        case e:IndexOutOfBoundsException => rows(i).t_action = 0.0
      }
    }
    (x._1,rows)
  }
  private def ip_analysis(ip: String) = {
    var (country, province, city) = ("未知", "未知", "未知")
    if (ip != null && ip.nonEmpty) {
      val ips = ip.split(",")
      val targetIP: String = if (ips.nonEmpty) ips(0) else ip
      if (targetIP.length > 0 && (targetIP.indexOf(".") != -1 || targetIP.indexOf(":") != -1)) {
        try {
          val inetAddress = InetAddress.getByName(ip)
          val geoResponse = geoIPResolver.city(inetAddress)
          country = geoResponse.getCountry.getNames.get("zh-CN")
          if (geoResponse.getSubdivisions.size() > 0) {
            province = geoResponse.getSubdivisions.get(0).getNames.get("zh-CN")
          }
          province = ip_provincename(province)


          city = geoResponse.getCity.getNames.get("zh-CN")
          if (city != null) {
            city = if (city.contains("市")) city else city + "市"
          } else city = "未知"
        } catch {
          case x: Throwable => () // println(x.getMessage) //println("内网IP：" + ip)
        }
      }
    }
    Array(country, province, city)
  }
  private def ip_provincename(province: String): String = {
    val allnameMap = Map("北京" -> "北京市", "京" -> "北京市", "天津" -> "天津市", "津" -> "天津市", "上海" -> "上海市", "沪" -> "上海市", "重庆" -> "重庆市",
      "渝" -> "重庆市", "河北" -> "河北省", "冀" -> "河北省", "河南" -> "河南省", "豫" -> "河南省", "湖北" -> "湖北省", "鄂" -> "湖北", "湖南" -> "湖南省", "湘" -> "湖南省",
      "江苏" -> "江苏省", "苏" -> "江苏省", "江西" -> "江西省", "赣" -> "江西省", "辽宁" -> "辽宁省", "辽" -> "辽宁省", "吉林" -> "吉林省", "吉" -> "吉林省", "黑龙江" -> "黑龙江省",
      "黑" -> "黑龙江省", "陕西" -> "陕西省", "陕" -> "陕西省", "秦" -> "陕西省", "山西" -> "山西省", "晋" -> "山西省", "山东" -> "山东省", "鲁" -> "山东省", "四川" -> "四川省",
      "川" -> "四川省", "蜀" -> "四川省", "青海" -> "青海省", "青" -> "青海省", "安徽" -> "安徽省", "皖" -> "安徽省", "海南" -> "海南省", "琼" -> "海南省", "广东" -> "广东省", "粤" -> "广东省",
      "贵州" -> "贵州省", "贵" -> "贵州省", "黔" -> "贵州省", "浙江" -> "浙江省", "浙" -> "浙江省", "福建" -> "福建省", "闽" -> "福建省", "台湾" -> "台湾省", "台" -> "台湾省",
      "甘肃" -> "甘肃省", "甘" -> "甘肃省", "陇" -> "甘肃省", "云南" -> "云南省", "云" -> "云南省", "滇" -> "云南省", "西藏" -> "西藏自治区", "藏" -> "西藏自治区", "宁" -> "宁夏回族自治区",
      "宁夏" -> "宁夏回族自治区", "广西" -> "广西壮族自治区", "桂" -> "广西壮族自治区", "新疆" -> "新疆维吾尔自治区", "新" -> "新疆维吾尔自治区", "内蒙古" -> "内蒙古自治区", "蒙" -> "内蒙古自治区",
      "香港" -> "香港特别行政区", "港" -> "香港特别行政区", "澳门" -> "澳门特别行政区", "澳" -> "澳门特别行政区")
    if (allnameMap.contains(province)) allnameMap(province) else province
  }
  private def readHadoopFile(hdfsFilePath: String): Array[Byte] = {
    import java.net.URI

    import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create(hdfsFilePath), conf)
    val path: Path = new Path(hdfsFilePath)
    if (fs.exists(path)) {
      val is: FSDataInputStream = fs.open(path)
      val stat: FileStatus = fs.getFileStatus(path)
      val buffer = new Array[Byte](String.valueOf(stat.getLen).toInt)
      is.readFully(0, buffer)
      is.close()
      buffer
    } else {
      throw new Exception("the file is not found .")
    }
  }



}
