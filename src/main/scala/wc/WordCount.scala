package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import scala.math.min

object WordCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)


    val threshold=20000


    val textFile = sc.textFile(args(0))
    val rawD = textFile.map(word => (word.split(",")(0),(1.0,word.split(",")(1)))).
      filter(s=>(Integer.valueOf(s._1)<=threshold)&&(if (s._2._2.equals("S")) {true} else Integer.valueOf(s._2._2)<=threshold))

    val graph=rawD.groupByKey().mapValues(_.toList)
    graph.persist()

    var distances = graph.mapValues(s=> if (s.contains((1.0,"S"))) 0.0 else Double.PositiveInfinity)
    val accum=sc.doubleAccumulator

    while (accum.isZero ){
      val temp=distances.collect()
      logger.info("Ronit")
      logger.info(accum.value)
      distances = graph.join(distances).flatMap(s=>helper(s._2._1,s._2._2,s._1)).reduceByKey((x,y)=>min(x,y))
      /* var diff = temp.subtractByKey(distances)
       if (diff.count()>=1){
         accum.add(1.0)
       }*/

      if (distances.collect() sameElements temp){
        accum.add(1.0)
      }
    }
    distances.saveAsTextFile(args(1))



  }

  def helper(values:List[(Double,String)],distance:Double,id:String) :List[(String,Double)] ={
    val list= values.map(s=>(s._2,s._1+distance))
    val toReturn =(id,distance)::list
    toReturn
  }



}