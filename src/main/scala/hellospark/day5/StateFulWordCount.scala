package hellospark.day5

import hellospark.day3.LoggerLevels
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 统计单词次数（加上之前批次统计的）
  * @author : xiaochai
  * @since : 2018/10/15
  */
object StateFulWordCount {

  //Seq这个批次某个单词的次数
  //Option[Int]：以前的结果

  //分好组的数据
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iter.map{case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0))}
  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    //StreamingContext
    val conf = new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //updateStateByKey必须设置setCheckpointDir
    sc.setCheckpointDir("F:\\wordcount")
    val ssc = new StreamingContext(sc, Seconds(5))

    val ds = ssc.socketTextStream("xiaochai", 8888)
    //DStream是一个特殊的RDD
    //hello tom hello jerry
    val result = ds.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
