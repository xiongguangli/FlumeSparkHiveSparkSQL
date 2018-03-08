package com.zzxyh
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
object LogAPP {
  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal = Calendar.getInstance()
    //cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    //println(yesterday)

    //1.创建SparkConf()并设置App名称
    //setMaster("local")本地模式
    val conf = new SparkConf().setAppName("log-app").setMaster("local")
    //2.SparkContext
    val sc = new SparkContext(conf)

    //3.创建一个RDD
    val logRDD = sc.textFile("hdfs://192.168.168.35:8022/nginx/logs/2018-03-02").map(line => {
      val pattern = new Regex("\".*?\"")
      val str = (pattern findAllIn line).mkString("\t").replace("\"", "") +
        "\t" + line.substring(line.lastIndexOf("\"") + 1, line.length).trim.split(" ").mkString("\t")
      val $http_x_forwarded_for = str.substring(0, str.indexOf("\t")).split(",")(0).trim
      val other_str = str.replace(str.substring(0, str.indexOf("\t")), "").trim
      val $server = other_str.substring(0, other_str.indexOf("\t")).replace(",", "\t").trim
      val other_sub_str = other_str.substring(other_str.indexOf("\t"), other_str.length).trim
      $http_x_forwarded_for + "\t" + $server + "\t" + other_sub_str

    }).map(line => line.split("\t"))
    //logRDD.foreach(println(_))

    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("http_x_forwarded_for", StringType, true),
        StructField("server_addr", StringType, true),
        StructField("server_name", StringType, true),
        StructField("remote_addr", StringType, true),
        StructField("remote_user", StringType, true),
        StructField("time_local", StringType, true),
        StructField("request", StringType, true),
        StructField("status", IntegerType, true),
        StructField("body_bytes_sent", DoubleType, true),
        StructField("http_referer", StringType, true),
        StructField("http_user_agent", StringType, true),
        StructField("request_time", DoubleType, true),
        StructField("request_length", DoubleType, true)

      )
    )
    //    //将RDD映射到rowRDD
    val rowRDD = logRDD.map(p => Row(
      p(0),
      p(1),
      p(2),
      if ("-".equals(p(0))) p(3) else p(0),
      p(5),
      p(6),
      p(7),
      p(8).toInt,
      p(9).toDouble,
      p(10),
      p(11),
      p(12).toDouble,
      p(13).toDouble)

    )
    //转换成dataFrame
    val sqlContext = new SQLContext(sc)
    //将schema信息应用到rowRDD上
    val logDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //logDataFrame.show()
    //注册表
    logDataFrame.registerTempTable("t_log")

    //统计访客数(UV)
    //访客数就是指一天之内到底有多少不同的用户访问了你的网站 按ip分组
    val uvDataFrame = sqlContext.sql("select count(distinct remote_addr) remote_addr from t_log")
    val uv = uvDataFrame.first()(0).toString.toInt
    //println("访客数:" + uv)
    //
    //访问次数
    //访问次数是指访客完整打开了网站页面进行访问的次数
    val vCountDataFrame = sqlContext.sql("select count(status) from t_log where status=200 ")
    val vCount = vCountDataFrame.first()(0).toString.toInt
    //println("访问次数 : " + vCount)

    //浏览量(PV)
    //浏览量和访问次数是呼应的。用户访问网站时每打开一个页面，就记为1个PV
    val pvDataFrame = sqlContext.sql("select count(*) from t_log")
    val pv = pvDataFrame.first()(0).toString.toInt
    //println("浏览量(PV):" + pv)

    //转化次数
    //潜在用户在我们的网站上完成一次我们期望的行为，就叫做一次转化。
    //期望的行为：用户成功访问主页一次 ：server_addr = "www.bxd365.com"
    //用户成功访问一个页面，定义为一次转化 status=200
    val changeDataFrame = sqlContext.sql("select count(*) from t_log where server_name = \"www.bxd365.com\" and status=200")
    //changeDataFrame.show()
    val change = changeDataFrame.first()(0).toString.toInt
    //println("转化次数:" + change)

    //转化率　
    //转化率=转化次数/访问次数。
    val changeRange = (change.toDouble / vCount.toDouble)
    //println("转化率:" + changeRange)

    //平均访问时长
    //平均访问时长是用户访问网站的平均停留时间。
    //平均访问时长=总访问时长/访问次数
    //总访问时长
    val totalTimeDataFrame = sqlContext.sql("select sum(request_time) from t_log")
    //changeDataFrame.show()
    val totalTime = totalTimeDataFrame.first()(0).toString.toDouble
    //println("总请求响应时长:" + totalTime)
    //平均访问时长
    val avgTime = totalTime / vCount
    //println("平均响应时长:" + avgTime)

    //什么叫平均访问页数
    //平均访问页数也是衡量网站的用户体验的指标
    //平均访问页数是用户访问网站的平均浏览页数。
    //平均访问页数=浏览量(PV)/访问次数
    val avgPageCount = pv.toDouble / vCount
    //println("平均访问页数:" + avgPageCount)


    //跳出率
    //跳出率=只访问一个页面就离开网站的访问次数/总访问次数，
    //select remote_addr from t_log group by remote_addr having count(remote_addr)<=1

    val vOnceDataFrame = sqlContext.sql("select remote_addr,count(server_name) s_count from t_log group by remote_addr having s_count<=1")

    //vOnceDataFrame.show()
    //跳出率
    val vOnceCount = vOnceDataFrame.count()
    val jumpRange = vOnceCount.toDouble / vCount
    //println(vOnceCount + "-" +  vCount )
    //println("跳出率:" + jumpRange)


    println("访客数:" + uv)
    println("访问次数 : " + vCount)
    println("浏览量(PV):" + pv)
    println("转化次数:" + change)
    println("转化率:" + changeRange)
    println("总请求响应时长:" + totalTime)
    println("平均响应时长:" + avgTime)
    println("平均访问页数:" + avgPageCount)
    println("跳出率:" + jumpRange)



    val resultRDD = sc.parallelize(List((uv, vCount, pv, change, changeRange, totalTime, avgTime, avgPageCount, jumpRange)))

    //通过StructType直接指定每个字段的schema
    val resultSchema = StructType(
      List(
        StructField("uv_", IntegerType, true),
        StructField("vCount_", IntegerType, true),
        StructField("pv_", IntegerType, true),
        StructField("change_", IntegerType, true),
        StructField("changeRange_", DoubleType, true),
        StructField("totalTime_", DoubleType, true),
        StructField("avgTime_", DoubleType, true),
        StructField("avgPageCount_", IntegerType, true),
        StructField("jumpRange_", DoubleType, true),
        StructField("log_data", DateType, true),//是哪一天日志分析出来的结果
        StructField("create_time", TimestampType, true)//分析结果的创建时间
      )
    )
    //将RDD映射到rowRDD
    val resultRowRDD = resultRDD.map(p => Row(
      p._1.toInt,
      p._2.toInt,
      p._3.toInt,
      p._4.toInt,
      p._5.toDouble,
      p._6.toDouble,
      p._7.toDouble,
      p._8.toInt,
      p._9.toDouble,
      new Date(cal.getTimeInMillis),//是哪一天日志分析出来的结果
      new Timestamp(new java.util.Date().getTime)
    ))
    //将schema信息应用到rowRDD上
    val resultDataFrame = sqlContext.createDataFrame(resultRowRDD, resultSchema)
    //注册表
    resultDataFrame.registerTempTable("t_result_log")
    //执行SQL
    val df = sqlContext.sql("select * from t_result_log")
    df.show()

    //将结果写入到mysql
    val prop = new java.util.Properties();
    prop.put("user", "root")
    prop.put("password", "123456")
    //将dataFrame中的数据写入到关系型数据库
    df.write.mode("append").jdbc("jdbc:mysql://hadoop-master:3306/hibernate", "t_log", prop)
    sc.stop()
  }
}
