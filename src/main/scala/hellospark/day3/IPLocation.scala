package hellospark.day3

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import hellospark.day3.LoggerLevels
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author : xiaochai
  * @since : 2018/10/13
  */
object IPLocation {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  val data2Mysql = (iterator: Iterator[(String,Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into location_info(location, counts, access_date) values(?,?,?)"

    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "17307867")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })

    }catch {
      case e: Exception => println("Mysql Exception")
    }finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setMaster("local[2]").setAppName("IpLocation")
    val sc = new SparkContext(conf)

    val ipRulesRdd = sc.textFile("f://ip.txt").map(line => {
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num, end_num, province)    })

    //全部的ip映射规则
    val ipRelesArray = ipRulesRdd.collect()

    //广播规则
    val ipRulesBroadcast = sc.broadcast(ipRelesArray)

    //加载要处理的数据
    val ipsRdd = sc.textFile("f://access_log.txt").map(line => {
      val fields = line.split("\\|")
      fields(1)
    })

    val result = ipsRdd.map(ip => {
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      //(ip的起始Num， ip的结束Num，省份名)
      info
    }).map(t => (t._3, 1)).reduceByKey(_+_)

    //向MySQL写入数据
    result.foreachPartition(data2Mysql(_))

    sc.stop()
  }
}
