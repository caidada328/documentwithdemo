package com.caicai.gmall.common.MY
import java.util.Properties


import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

object MyEsutil2 {
  val esUrl = "http://hadoop104:8300"
  val factory = new JestClientFactory

  val conf = new HttpClientConfig.Builder(esUrl)
    .maxTotalConnection(100)
    .connTimeout(10*100)
    .readTimeout(10*1000)
    .multiThreaded(true)
    .build()

  factory.setHttpClientConfig(conf)

  def getClient = factory.getObject

  def insertSingle(index:String,source:Object,id:String = null) = {
    val client = factory.getObject
    val action = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()

    client.execute(action)
    client.shutdownClient()
  }

  def insertBulk(index:String,sources:Iterator[Any]) = {
    val client = factory.getObject
    val bulk = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")

    sources.foreach({
      case(id:String,data) =>
      val action = new Index.Builder(data).id(id).build()
      bulk.addAction(action)
      case data => val action2 = new Index.Builder(data).build()
      bulk.addAction(action2)
    })
    client.execute(bulk.build())
    client.shutdownClient()
  }


}
