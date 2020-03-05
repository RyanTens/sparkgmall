package com.tens.dw.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.tens.dw.gmall.common.Constant
import com.tens.dw.gmall.realtime.bean.{AlertInfo, EventLog}
import com.tens.dw.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AlterApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setAppName("AlterApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val rawStream: DStream[(String, String)] =
      MyKafkaUtil.getKafkaStream(ssc, Constant.EVENT_TOPIC).window(Minutes(5))
    val eventStream: DStream[(String, EventLog)] = rawStream.map {
      case (_, jsonString) =>
        val log: EventLog = JSON.parseObject(jsonString, classOf[EventLog])
        (log.mid, log)
    }
    val alertInfoStream: DStream[(Boolean, AlertInfo)] = eventStream.groupByKey
      .map {
        case (mid, logIt) =>
          val uidSet = new util.HashSet[String]()
          val eventList: util.List[String] = new util.ArrayList[String]()
          val itemSet = new util.HashSet[String]()
          var isClickItem = false;
          import scala.util.control.Breaks._
          breakable {
            logIt.foreach(log => {
              eventList.add(log.eventId)
              log.eventId match {
                case "coupon" =>
                  uidSet.add(log.uid)
                  itemSet.add(log.itemId)
                case "clickItem" =>
                  isClickItem = true
                  break
                case _ =>
              }
            })
          }
          (uidSet.size() >= 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
      }
    alertInfoStream.filter(_._1).foreachRDD(rdd => {
      rdd
    })
    alertInfoStream.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}
