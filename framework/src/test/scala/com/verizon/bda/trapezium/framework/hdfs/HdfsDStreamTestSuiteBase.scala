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
package com.verizon.bda.trapezium.framework.hdfs

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.handler.StreamingHandler
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import com.verizon.bda.trapezium.validation.Validator
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.Suite
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}

/**
 * @author Pankaj on 4/15/16.
 */
class HdfsDStreamTestSuiteBase extends TestSuiteBase{
  self: Suite =>
  @transient var ssc: StreamingContext = _
  @transient var sparkConf: SparkConf = _
  private var zk: EmbeddedZookeeper = _
  private var appConfig: ApplicationConfig = _

  val logger = LoggerFactory.getLogger(this.getClass)

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load the config file
    appConfig = ApplicationManager.getConfig()
    // set up ZooKeeper Server
    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))

  }

  override def afterAll() {
    if (ssc != null) {
      ssc.stop(true, true)
    }
    super.afterAll()

    // Close ZooKeeper connections
    ZooKeeperConnection.close

    // shutdown ZooKeeper Server
    if (zk != null) {
      zk.shutdown()
      zk = null
    }

  }

  def applicationManagerProcess(inputStream: DStream[String]): Unit = {


    val inputRowStream: DStream[Row] = inputStream.map(line => Row(line))

    val workflowConfig = ApplicationManager.getWorkflowConfig
    val hdfsStream = workflowConfig.hdfsStream.asInstanceOf[Config]
    val streamsInfo = hdfsStream.getConfigList("streamsInfo")

    val streamInfo = streamsInfo.get(0)
    val streamName = streamInfo.getString("name")
    logger.debug("streamName - " + streamName)
    val validatedDStream = Validator.getValidatedStream(streamName, inputRowStream, streamInfo)
    logger.debug("streamName - " + validatedDStream)



    StreamingHandler.handleWorkflow( MMap(streamName -> validatedDStream))
  }

  def setupWorkflow(workflowName: String, inputStream: Seq[Seq[String]],
                    batchTime : Long = System.currentTimeMillis() ): Unit = {

    ApplicationManager.setWorkflowConfig(workflowName)
    ApplicationManager.updateWorkflowTime(batchTime)

    val ssc = setupActionStreams(inputStream, applicationManagerProcess)
    runActionStreams(ssc, inputStream.size)

  }

}
