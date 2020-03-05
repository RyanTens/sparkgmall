package com.tens.dw.gmall.canal

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.tens.dw.gmall.common.Constant

object CanalHandler {
  import scala.collection.JavaConversions._
  def handle(tableName: String, rowDatas: util.List[CanalEntry.RowData], eventType: CanalEntry.EventType) = {
    if("order_info" == tableName && rowDatas != null && !rowDatas.isEmpty && eventType == EventType.INSERT){
      for(rowData <- rowDatas){
        val jsonObj = new JSONObject()
        val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        for(column <- columns){
          val key: String = column.getName
          val value: String = column.getValue
          jsonObj.put(key, value)
        }
        MyKafkaUtil.send(Constant.ORDER_TOPIC, jsonObj.toJSONString)

      }
    }
  }


}
