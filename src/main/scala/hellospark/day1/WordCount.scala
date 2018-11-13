package hellospark.day1

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author : xiaochai
  * @since : 2018/10/11
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("F://menu.txt").flatMap(_.split("/")).map((_,1))
        .reduceByKey(_+_).sortBy(_._2, false)

    println(rdd.collect().toBuffer)
    sc.stop()
  }
}
