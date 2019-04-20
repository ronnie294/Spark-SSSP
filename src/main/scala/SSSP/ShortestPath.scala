package SSSP

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.min

object ShortestPath {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SSSP")
    val sc = new SparkContext(conf)

/*
Threshold to test for various cases
 */
    val threshold=11316811

    val textFile = sc.textFile(args(0))
    val rawD = textFile.map(word => (word.split(",")(0),(1.0,word.split(",")(1)))).
      filter(s=>(Integer.valueOf(s._1)<=threshold)&&(if (s._2._2.equals("S")) {true} else Integer.valueOf(s._2._2)<=threshold))

/*
Construction and persisting of graph
 */

    val graph=rawD.groupByKey().mapValues(_.toList)
    graph.persist()

    var distances = graph.mapValues(s=> if (s.contains((1.0,"S"))) 0.0 else Double.PositiveInfinity)
    val accum=sc.doubleAccumulator

   /*
   Accumulator is used to detect convergence
    */
    while (accum.isZero ){
      val temp=distances
      distances = graph.join(distances).flatMap(s=>helper(s._2._1,s._2._2,s._1)).reduceByKey((x,y)=>min(x,y))

      var count = temp.subtract(distances).count()
      logger.info(count+"Ronit")

      if (count==0){
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