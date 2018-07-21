import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestSparkStreaming {
  def main(args: Array[String]): Unit = {
    var t = 0
    def func(x: String): Unit = {
      val xtime = x.toString().split(" ")(0).toInt
      val value = x.toString().split(" ")(1)
      val gap = xtime - t
      println(xtime,gap,value)
      t = xtime
    }
    val sc = new SparkConf().setAppName("test").setMaster("local[8]")
    val ssc = new StreamingContext(sc,Seconds(20))
    val batch = ssc.socketTextStream("localhost",9999)
    batch.map(x =>func(x)).count()
    ssc.start()
    ssc.awaitTermination()
  }

}
