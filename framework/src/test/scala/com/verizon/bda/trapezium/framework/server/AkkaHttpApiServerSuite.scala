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

import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory

/**
 * Created by Pankaj on 8/10/16.
 */
class AkkaHttpApiServerSuite extends FunSuite with BeforeAndAfterAll {

  val logger = LoggerFactory.getLogger(this.getClass)

  var appConfig: ApplicationConfig = _
  var zk: EmbeddedZookeeper = null

  val args = Array("--workflow", "akkaHttpApiServer")

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
      ApplicationManager.getEmbeddedServer.stop(true)
    }
  }

  test("configured end-points should work for API tests") {
    val client = new HttpClient
    val method1 = new GetMethod("http://localhost:8090/rest-api/test")

    client.executeMethod(method1)
    assert(method1.getResponseBodyAsString.contains("Hello from EndPoint 1"))

    val method2 = new GetMethod("http://localhost:8090/rest-api/test2")

    client.executeMethod(method2)
    assert(method2.getResponseBodyAsString.contains("Hello from EndPoint 2"))
  }

}
