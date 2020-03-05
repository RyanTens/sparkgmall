package com.tens.dw.gmall.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString

object CanalClient {
  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConversions._
    val address = new InetSocketAddress("hadoop101", 11111)

    val connector: CanalConnector =
      CanalConnectors.newSingleConnector(address, "example", "", "")
    connector.connect()

    connector.subscribe("gmall.*")

    while (true) {
      val msg: Message = connector.get(100)
      val entries: util.List[CanalEntry.Entry] = msg.getEntries
      if (entries != null && !entries.isEmpty) {

        for (entry <- entries) {
          if(entry != null && entry.getEntryType == EntryType.ROWDATA){

            val storeValue: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            val rowDatas: util.List[CanalEntry.RowData] =
              rowChange.getRowDatasList

            CanalHandler.handle(
              entry.getHeader.getTableName,
              rowDatas,
              rowChange.getEventType
            )
          }
        }
      }else {
        println("没有拉取到数据，2s后继续。。。")
        Thread.sleep(2000)
      }
    }
  }

}
