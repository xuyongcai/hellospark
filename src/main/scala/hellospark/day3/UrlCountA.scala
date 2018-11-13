package hellospark.day3

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author : xiaochai
  * @since : 2018/10/11
  */
object UrlCountA {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountA").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("F:\\menu.txt").map(x => {
      val arr = x.split("/")
      (arr(1),1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(t => {
      val name = t._1
      val count = t._2
      (name, count)
    })

    val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._2).reverse.take(3)
    })

    println(rdd4.collect().toBuffer)
    sc.stop()
  }
}
