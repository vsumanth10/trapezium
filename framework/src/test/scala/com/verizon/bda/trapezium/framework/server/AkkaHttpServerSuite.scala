/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.verizon.bda.trapezium.framework.server

import akka.http.scaladsl.model.StatusCodes
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory

/**
  * Created by Jegan on 5/20/16.
  */
class AkkaHttpServerSuite extends FunSuite with BeforeAndAfterAll {

  val logger = LoggerFactory.getLogger(this.getClass)

  var appConfig: ApplicationConfig = _
  var zk: EmbeddedZookeeper = null

  val args = Array("--workflow", "akkaHttpServerWorkFlow")

  override def beforeAll(): Unit = {
    appConfig = ApplicationManager.getConfig()

    // set up ZooKeeper Server
    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))
    ApplicationManager.main(args)
  }

  override def afterAll: Unit = {
    // Close ZooKeeper connections
    ZooKeeperConnection.close

    if (zk != null) {
      zk.shutdown()
      zk = null
    }

    // stop HTTP server if started
    if( ApplicationManager.getEmbeddedServer != null) {

      logger.info(s"Stopping embedded server")
      ApplicationManager.getEmbeddedServer.stop()
    }
  }

  test("configured end-points should work") {
    val client = new HttpClient
    val method1 = new GetMethod("http://localhost:8090/rest-api/test")

    client.executeMethod(method1)
    assert(method1.getResponseBodyAsString.contains("Hello from EndPoint 1"))

    val method2 = new GetMethod("http://localhost:8090/rest-api/test2")

    client.executeMethod(method2)
    assert(method2.getResponseBodyAsString.contains("Hello from EndPoint 2"))
  }

  test("exception should be handled by the framework") {
    val client = new HttpClient
    val method = new GetMethod("http://localhost:8090/rest-api/test3")

    client.executeMethod(method)
    assert(method.getStatusCode == StatusCodes.InternalServerError.intValue)
  }

  test("should support actor-based endpoints") {
    val client = new HttpClient
    val method = new GetMethod("http://localhost:8090/rest-api/actortest")

    client.executeMethod(method)
    assert(method.getResponseBodyAsString.contains("Hello from Actor based EndPoint 4"))
  }

}
