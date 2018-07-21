package com.ubisoft.deposit

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.Map
import scala.util.parsing.json.JSONObject
object Deposit {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val theta = 0.001
  val delta = 0.15
  val pIDtargetRate = 0.06/(1-2*theta)

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


  def rVal(previous_amount: Double, amount: Double, previous_rate: Double) = {
    previous_rate*previous_amount*(1+delta)/amount
  }

  /**
    * map函数
    *
    * @param x
    */
  def func(x: (String, Double, Double, Long, Int, Int, Int, Double, Double, Double, Double),product_id:String,dt:String,renewalPeriod:Int)={
    val user_id = x._1
    val depCnt = x._4
    val rgActD = x._5
    val act_30 = x._6
    val act_7 = x._7
    val t_median = x._8.toFloat
    val depSum_tar = x._9.toFloat
    val wdSum_tar = x._10.toFloat
    val netProfitRate = x._11.toFloat

    var beta = x._2
    var alpha = x._3
    var mode = (x._3-1)/x._2
    var median = x._3 * (15 * x._3 - 4) / (x._2 * (15 * x._3 + 1))
    var bdry = alpha/beta
    if(alpha >1) {
      bdry = mode + scala.math.sqrt(x._3 - 1) / x._2
    }else if(median >0 ){
      bdry = median
    }

    var next_bdry = bdry + (bdry/(beta*bdry - alpha + 1))
    bdry = (1+(bdry/100).toInt)*100
    next_bdry = (1+(next_bdry/100).toInt)*100

    val sub_missions:Map[Int,Tuple2[Double,Int]]=Map()
    val num_sub_missions = next_bdry.toInt.toString.length - 2

    val r_0 = Array(0.05,(0.5-theta)*netProfitRate).min
    var previous_r = 0.0

    for (j <- 0 until num_sub_missions + 2){
      var tmp = (j*next_bdry + (num_sub_missions+1-j)*bdry)/(num_sub_missions+1).toDouble
      tmp = (1+(tmp/100).toInt)*100
      var r_tmp = r_0
      if(j ==0){
        val b = (tmp*r_0/50).toInt*10+8
        if(b<=0){
          sub_missions(0) = (tmp, (tmp*0.001).toInt*10 + 8)
        }else{
          sub_missions(0) = (tmp,b)
        }
      }else {
        r_tmp = rVal(sub_missions(j-1)._1,tmp,previous_r)
        val b = (tmp*r_tmp/50).toInt*10+8
        if(b<=0){
          sub_missions(j) = (tmp,(tmp*0.001).toInt*10+8)
        }else{
          sub_missions(j) = (tmp,b)
        }
      }
      previous_r =r_tmp
    }
    var budget =0
    sub_missions.foreach(x => budget += x._2._2)
    var sum_1 = 0.0
    sub_missions.foreach(x => sum_1 += x._2._1)
    val expectedEarn =sum_1*netProfitRate - budget
    val sub_missions_string = "{" + sub_missions.map({ case (k,v) => "\""+k.toString + "\":" +"{\"deposit\":" +v._1.toInt+","+"\"bonus\":"+v._2+"}"}).mkString(",") + "}"
    //返回结果
    (user_id,beta,alpha,mode,median,bdry,next_bdry,sub_missions_string,budget,depCnt,rgActD,act_30,act_7,t_median,depSum_tar,wdSum_tar,netProfitRate,expectedEarn,product_id,dt,renewalPeriod)
  }
  /**
    * 获得统计数据
    *
    * @param x
    * @return
    */
  def getDescriptiveStatistics(x: Row) ={
    val L_dep = x.getSeq[Double](1)
    val ds = new DescriptiveStatistics()
    for(i <- L_dep) {ds.addValue(i)}
    val mean = ds.getMean
    val vari = ds.getVariance
    var alpha = mean*mean/vari
    if(vari == 0){alpha = -1}
//    (x.getString(0),ds.getMean,ds.getVariance,x.getInt(2),x.getInt(3),x.getInt(4),x.getInt(5),ds.getPercentile(50),x.getFloat(6),x.getFloat(7),x.getFloat(8))
    (x.getString(0),mean/vari,alpha,x.getAs[Long](2),x.getAs[Int](3),x.getAs[Int](4),x.getAs[Int](5),ds.getPercentile(50),x.getAs[Double](6),x.getAs[Double](7),x.getAs[Double](8))
  }
  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /**
      * 设置参数
      */
    val dt = args(0).trim //为程序执行当天日期
    val pID = args(1).trim
    val renewalPeriod = args(2).trim.toInt
    val targetDate =getNDayAgo(dt,renewalPeriod)
    val yesterday = getNDayAgo(dt,1)
    val sevenDaysAgo = getNDayAgo(dt,7)
    val thirtyDaysAgo = getNDayAgo(dt,30)
    val threeMonthsAgo = getNDayAgo(dt,90)

    /**
      * 建立spark Session
      */
    val spark = SparkSession.builder()
      .appName("deposit")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    /**
      * 建立mysql连接
      */
    import java.util.Properties
    val prop = new Properties()
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    prop.put("user","mldm")
    prop.put("password","mldm123")
    val url ="jdbc:mysql://10.180.33.43:3306/mldm"

    /**
      * 删除MySQL中历史数据
      */
    val sql = String.format("delete from deposit_activity where product_id = '%s' and dt = '%s' and renewal_period = %s ",pID,dt,renewalPeriod.toString)
    println(sql)
    val conn_str = "jdbc:mysql://10.180.33.43:3306/mldm?user=mldm&password=mldm123"
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(conn_str)
    try {
      val statement = conn.createStatement()
      statement.execute(sql)
    }catch {
      case e:Exception =>e.printStackTrace()
    }finally {conn.close()}
    println("#########################finish mysql clean #################################")


    /**
      * spark ETL
      */
    import spark.implicits._
    spark.sql("use ibds_dw")
    val dosql = String.format("select a.user_fk,a.L_dep,a.cnt,b.dDiff from (select user_fk,collect_list(dep) as L_dep,count(1) as cnt from (" +
      "select user_fk,dt,sum(deposit_amount) dep from t_users_deposit_hourly_fact where dt>='%s' and dt <= '%s' and split(user_fk,'_')[0] = '%s' and substr(user_fk,5,1)='r'  group by user_fk,dt) t group by user_fk) a" +
      " left join (select user_id,datediff(cast(current_date as string),split(register_time,'T')[0]) as dDiff from t_users_dim where dt='%s' and product_id='%s' and customer_type=1 and substr(user_id,5,1)='r') b  " +
      "on a.user_fk = b.user_id",targetDate,yesterday,pID,yesterday,pID)
    val dosql_2 = String.format("select p.user_fk,p.cnt_30,q.cnt_7 from (select user_fk,size(collect_set(dt)) as cnt_30 from t_users_login_hourly_fact where dt>='%s' and dt <= '%s' and split(user_fk,'_')[0]='%s' and " +
      "substr(user_fk,5,1)='r' group by user_fk) p left join (select user_fk,size(collect_set(dt)) as cnt_7 from t_users_login_hourly_fact where dt>='%s' and dt <= '%s' and split(user_fk,'_')[0]='%s' and substr(user_fk,5,1)='r' " +
      "group by user_fk) q  on  p.user_fk=q.user_fk",thirtyDaysAgo,yesterday,pID,sevenDaysAgo,yesterday,pID)
    val dosql_3 = String.format("select p.user_fk, p.depSum, nvl(q.wdSum,0) as wdSum, (p.depSum - nvl(q.wdSum,0))/p.depSum as netProfitRate from (select user_fk,sum(deposit_amount) as depSum from t_users_deposit_hourly_fact " +
      "where dt>='%s' and dt <= '%s' and split(user_fk,'_')[0]='%s' and substr(user_fk,5,1)='r' group by user_fk ) p left join ( select user_fk,sum(withdraw_amount) as wdSum from t_users_withdraw_hourly_fact where dt>='%s' " +
      "and dt <='%s' and split(user_fk,'_')[0]='%s' and substr(user_fk,5,1)='r' group by user_fk ) q on p.user_fk=q.user_fk",threeMonthsAgo,yesterday,pID,threeMonthsAgo,yesterday,pID)
    val df = spark.sql(dosql).join(spark.sql(dosql_2),Seq("user_fk"),"left_outer").na.fill(0).join(spark.sql(dosql_3),Seq("user_fk"),"left_outer").filter("cnt>1")
      .filter("cnt >10 or dDiff<30").rdd.map(x =>getDescriptiveStatistics(x)).filter(_._3 != -1).map(x =>func(x,pID,dt,renewalPeriod))
      .toDF("user_id","beta","alpha","mode","median","bdry","next_bdry","missions","budget","depCnt","rgActD","active30Days","active7Days","dep_median"
      ,"dep_sum","withdraw_sum","netProfitRate","expectedEarn","product_id","dt","renewal_period").filter("bdry < next_bdry")
    df.write.mode(SaveMode.Append).jdbc(url,"deposit_activity",prop)
    println("-----------END-------------")
  }

}
