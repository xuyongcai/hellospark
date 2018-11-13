package hellospark.day1

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author : xiaochai
  * @since : 2018/10/15
  */
object UserLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("F://menu.txt").map( x => {
      val arr = x.split("/")
      (arr(1),1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)

    println(rdd2.collect().toBuffer)
  }
}
