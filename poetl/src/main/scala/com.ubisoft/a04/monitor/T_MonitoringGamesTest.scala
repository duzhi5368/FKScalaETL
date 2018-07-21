package com.ubisoft.a04.monitor
import java.text.SimpleDateFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
object T_MonitoringGamesTest {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'")
  def main(args: Array[String]): Unit = {
    var str = "aaa"
    str = "bbb"
    val spark = SparkSession.builder()
      .appName("T_MonitoringGames")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    val snapshot_date  = args(0).trim
    import spark.implicits._
    val bc =spark.sparkContext.broadcast(str)
    val df = spark.sql(String.format("select referer from test.t_monitor_games_staging  where snapshot_date  = '%s' ",snapshot_date))
    df.rdd.map(x =>func(x)).toDF("name","age").show()
    spark.close()
    def func(x: Row) = {
      val ip = "10.180.55.69"
      (bc,1)
    }
  }
}
