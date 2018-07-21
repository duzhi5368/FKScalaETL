import com.alibaba.fastjson.JSON
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

import scala.collection.mutable.Map
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.parsing.json.{JSONFormat, JSONObject}
object TestMath {
  def main(args: Array[String]): Unit = {
    val ds = new DescriptiveStatistics()
    for(i<-0 to 1000){ds.addValue(i)}
    val mean = ds.getMean
    println(ds.getPercentile(95))
//    val vari = ds.getPopulationVariance
//    val x = (0,mean/vari,mean*mean/vari)
//    var beta = x._2
//    var alpha = x._3
//    var mode = (x._3-1)/x._2
//    var median = x._3 * (15 * x._3 - 4) / (x._2 * (15 * x._3 + 1))
//    var bdry = alpha/beta
//    if(alpha >1) {
//      bdry = mode + scala.math.sqrt(x._3 - 1) / x._2
//    }else if(median >0 ){
//      bdry = median
//    }

//    var next_bdry = bdry + (bdry/(beta*bdry - alpha + 1))
//    bdry = (1+(bdry/100).toInt)*100
//    next_bdry = (1+(next_bdry/100).toInt)*100
  }
  def map2String(obj:Map[Int,Tuple2[Double,Int]]):String = {
      "{" + obj.map({ case (k,v) => k.toString + ":" + v}).mkString(",") + "}"
  }

}
