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



  }



}