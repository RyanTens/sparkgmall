package com.tens.dw.gmall.realtime.app

import com.tens.dw.gmall.common.Constant
import com.tens.dw.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AlterApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val rawStream: DStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_TOPIC).window(Minutes(5))

    rawStream.map{
      case (_, jsonString) =>

    }

    ssc.start()
    ssc.awaitTermination()
  }
}
