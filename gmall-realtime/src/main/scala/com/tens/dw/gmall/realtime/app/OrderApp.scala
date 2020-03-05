package com.tens.dw.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.tens.dw.gmall.common.Constant
import com.tens.dw.gmall.realtime.bean.OrderInfo
import com.tens.dw.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setAppName("OrderApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream: InputDStream[(String, String)] =
      MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_TOPIC)

    val orderInfoStream: DStream[OrderInfo] = sourceStream.map {
      case (_, jsonString) =>
        JSON.parseObject(jsonString, classOf[OrderInfo])

    }
    orderInfoStream.print(1000)
    import org.apache.phoenix.spark._
    orderInfoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq(
          "ID",
          "PROVINCE_ID",
          "CONSIGNEE",
          "ORDER_COMMENT",
          "CONSIGNEE_TEL",
          "ORDER_STATUS",
          "PAYMENT_WAY",
          "USER_ID",
          "IMG_URL",
          "TOTAL_AMOUNT",
          "EXPIRE_TIME",
          "DELIVERY_ADDRESS",
          "CREATE_TIME",
          "OPERATE_TIME",
          "TRACKING_NO",
          "PARENT_ORDER_ID",
          "OUT_TRADE_NO",
          "TRADE_BODY",
          "CREATE_DATE",
          "CREATE_HOUR"
        ),
        zkUrl = Some("hadoop101,hadoop102,hadoop103:2181")
      )
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
