package hellospark.day5

import hellospark.day3.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author : xiaochai
  * @since : 2018/10/15
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("FlumeWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //hostname是本地的虚拟机的地址 运行sparkstreaming，并接收flume的推送
    val flumeStream = FlumeUtils.createStream(ssc, "10.2.181.251", 8888)

    //flume中的数据通过event.getBody()才能拿到真正的内容
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_, 1))

    val results = words.reduceByKey(_+_)
    results.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
