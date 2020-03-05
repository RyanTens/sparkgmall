package com.tens.dw.gmall.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

object EsUtil {

  val esUrl = "http://hadoop101:9200"
  //1.创建es客户端工厂
  val factory = new JestClientFactory
  var conf: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
    .maxTotalConnection(100)
    .connTimeout(1000 * 100)
    .readTimeout(1000 * 100)
    .multiThreaded(true)
    .build()
  factory.setHttpClientConfig(conf)

  //返回一个客户端
  def getClient = factory.getObject

  //case class User(age: Int, name: String)

//  def main(args: Array[String]): Unit = {
////    insertSingle("user", """
////        |{
////        |"age": 18,
////        |"name": "ryan"
////        |}
////        |""".stripMargin, "100")
//
//    insertBulk(
//      "user",
//      List((User(10, "a"), "aaaaaa"), User(20, "b"), User(30, "c")).toIterator
//    )
//  }

  def insertBulk(index: String, sources: Iterator[Any]): Unit = {
    val client = getClient
    val bulkBuider: Bulk.Builder = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")
    sources.foreach({
      case (s, id: String) =>
        val action: Index = new Index.Builder(s).id(id).build()
        bulkBuider.addAction(action)
      case s =>
        val action: Index = new Index.Builder(s).build()
        bulkBuider.addAction(action)
    })

    client.execute(bulkBuider.build())
    client.shutdownClient()
  }

  def insertSingle(index: String, source: Any, id: String = null) = {
    val client = getClient
    val action = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()
    client.execute(action)
    client.shutdownClient()
  }

}


