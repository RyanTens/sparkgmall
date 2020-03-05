package com.tens.dw.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.tens.dw.gmall.common.Constant
import com.tens.dw.gmall.realtime.bean.StartupLog
import com.tens.dw.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.从kafka读取数据
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val rawStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_TOPIC)
    //2.把数据解析 封装到样例类中
    val startupLogStream: DStream[StartupLog] = rawStream.map {
      case (_, v) => JSON.parseObject(v, classOf[StartupLog])
    }

    //3.去重
    //3.1先从redis中读取语句启动的记录，把启动的过滤掉
    val filteredStream: DStream[StartupLog] = startupLogStream.transform(rdd => {
      //3.2 读取redis中已经启动的记录
      val client: Jedis = RedisUtil.getJedisClient
      val midSet: util.Set[String] = client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      client.close()
      val bd: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
      //3.3 过滤掉那些已经启动过的设备
      rdd.filter(log => {
        !bd.value.contains(log.mid)
      }).map(log => (log.mid, log))
        .groupByKey()
        .map{
          case (_, logIt) => logIt.toList.minBy(_.ts)
        }
      //在一个时间段内，一个设备启动了两次
    })
    //3.4 把第一次启动的设备mid'写入到redis
    filteredStream.foreachRDD(rdd => {
      rdd.foreachPartition(logIt => {
        //获取连接
        val client = RedisUtil.getJedisClient
        //写入到redis中
        logIt.foreach(log => {
          //一次写一个
          client.sadd(Constant.STARTUP_TOPIC + log.logDate, log.mid)
        })
        //关闭连接
        client.close()
      })
    })

    filteredStream.print

    //4.写入hbase中
    filteredStream.foreachRDD(rdd => {
      //1.提前在pheonix中创建要保存的数据

      //2. 直接保存
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop101,hadoop102,hadoop103:2181")
      )


    })

    ssc.start()
    ssc.awaitTermination()
  }
}
