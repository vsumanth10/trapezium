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
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig}
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory

/**
 * Created by Pankaj on 5/3/16.
 */
class JettyServerSuite extends FunSuite with BeforeAndAfter {

  val logger = LoggerFactory.getLogger(this.getClass)

  var appConfig: ApplicationConfig = _
  var zk: EmbeddedZookeeper = null

  before{

    appConfig = ApplicationManager.getConfig()

    // set up ZooKeeper Server
    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))
  }

  after{

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

  test("HTTP Server test"){

    val args = Array("--workflow", "jettyServerWorkFlow")
    ApplicationManager.main(args)

    val client = new HttpClient
    val method1 = new PostMethod("http://localhost:8090/1")
    val method2 = new PostMethod("http://localhost:8090/2")

    client.executeMethod(method1)
    assert(method1.getResponseBodyAsString.contains("Servlet 1"))

    client.executeMethod(method2)
    assert(method2.getResponseBodyAsString.contains("Servlet 2"))
  }
}
