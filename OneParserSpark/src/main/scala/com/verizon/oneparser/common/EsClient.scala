
package com.verizon.oneparser.common

import java.net.{InetAddress, UnknownHostException}
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{TransportAddress}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import com.verizon.oneparser.common.Constants.LocalTransportAddress

object EsClient extends LazyLogging {
  var client: TransportClient = null

  /*
   * static { Settings settings = Settings.builder().put("cluster.name",
   * "dmat_es").build(); try { client =
   * extracted(settings).addTransportAddress( (TransportAddress) new
   * TransportAddress(InetAddress.getByName("10.20.40.183"), 9300)); } catch
   * (UnknownHostException e) { log.error(
   * "Unable to connect to ES repository"); throw new InfoPointsException(); }
   *
   * }
   */
  /**
    * @param settings
    * @return
    */
  private def extracted(settings: Settings) = new PreBuiltTransportClient(settings)


  private def getAddress(hosts: Array[String]) = {
    val list = new util.ArrayList[TransportAddress]
    for (host <- hosts) {
      var hostPort = host.split(":")
      list.add(new TransportAddress(InetAddress.getByName(hostPort(0)), hostPort(1).toInt))
    }
    list.toArray(new Array[TransportAddress](list.size))
  }
  def build(localTrasportClient: LocalTransportAddress): Unit ={
    var nodes:Array[String] = null
    val nodeString = localTrasportClient.host + ":" + localTrasportClient.port
    if (nodeString.contains(",")) nodes = nodeString.split(",")
    else nodes = Array[String](nodeString)
    val settings = Settings.builder.put("cluster.name", localTrasportClient.clusterName).build
    try {
      System.setProperty("es.set.netty.runtime.available.processors", "false")
      client = extracted(settings).addTransportAddresses(getAddress(nodes)(0))
    }
    catch {
      case e: UnknownHostException =>
        logger.error("Unable to connect to ES repository")
        throw new UnknownHostException(localTrasportClient.host)
    }
  }
}

