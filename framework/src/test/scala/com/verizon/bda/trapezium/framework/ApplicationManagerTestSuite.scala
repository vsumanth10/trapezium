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
package com.verizon.bda.trapezium.framework

import com.typesafe.config.{ConfigList, ConfigObject}
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import com.verizon.bda.trapezium.validation.Validator
import org.apache.spark.sql.Row
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.zookeeper.EmbeddedZookeeper

import scala.collection.mutable.{Map => MMap}

/**
 * @author debasish83 test utilities for application manager, embedded kafka and zk.
 */
class ApplicationManagerTestSuite extends TestSuiteBase {
  var appConfig: ApplicationConfig = _
  var zk: EmbeddedZookeeper = _

  override def beforeAll() {
    super.beforeAll()
    appConfig = ApplicationManager.getConfig()
    // set up ZooKeeper Server
    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))
    // load start up class
    ApplicationManager.initialize(appConfig)
  }

  override def afterAll() {
    // Close ZooKeeper connections
    ZooKeeperConnection.close

    // shutdown ZooKeeper Server
    if (zk != null) {
      zk.shutdown()
    }
    super.afterAll()
  }

  /**
   * Creates validated dstream
   * @param streamsInfo
   * @param inputStream
   * @return
   */
  def getValidatedDStream(streamsInfo: ConfigList,
                          inputStream: DStream[String]): MMap[String, DStream[Row]] = {
    val dStreams = collection.mutable.Map[String, DStream[Row]]()

    val streamsInfoListItr = streamsInfo.iterator
    while (streamsInfoListItr.hasNext) {
      val streamInfo = streamsInfoListItr.next.asInstanceOf[ConfigObject].toConfig
      val streamName = streamInfo.getString("name")
      val inputRowStream = inputStream.map(line => Row(line))
      val validatedDStream =
        Validator.getValidatedStream(
          streamName,
          inputRowStream,
          streamInfo)
      dStreams += ((streamName, validatedDStream))
    }
    dStreams
  }
}
