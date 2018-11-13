package hellospark.day3

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  *
  * @author : xiaochai
  * @since : 2018/10/14
  */
object UrlCountPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("f://url.txt").map(line => {
      val f = line.split("\\|")
      (f(0), 3)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    }).cache()//cache会将数据缓存到内存当中，cache是一个Transformation，lazy

    val ints = rdd3.map(_._1).distinct().collect()

    val hostParitioner = new HostParitioner(ints)

    val rdd4 = rdd3.partitionBy(hostParitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    rdd4.saveAsTextFile("f://out")

    //println(rdd4.collect().toBuffer)
    sc.stop()
  }

}

/**
  * 决定了数据到哪个分区里面
  * @param ins
  */
class HostParitioner(ins: Array[String]) extends Partitioner {
  val parMap = new mutable.HashMap[String, Int]()
  var count = 0
  for(i <- ins){
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString, 0)
  }
}


