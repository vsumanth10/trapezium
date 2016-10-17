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
package com.verizon.bda.trapezium.framework.handler

import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import com.verizon.bda.trapezium.validation.{ValidationConfig, DataValidator}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.sql.{SQLContext, Row}
import org.scalatest.FunSuite
import org.apache.spark.zookeeper.EmbeddedZookeeper
import com.verizon.bda.trapezium.framework.ApplicationManager

/**
 * @author sumanth.venkatasubbaiah
 *         Test suite for BatchHandler
 */
class BatchHandlerSuite extends FunSuite  with LocalSparkContext {

  var appConfig: ApplicationConfig = _
  var workflowConfig: WorkflowConfig = _

  val path1 = "src/test/data/parquet"
  val path2 = "src/test/data/hdfs/source2/"
  var zk: EmbeddedZookeeper = null

  override def beforeAll(): Unit = {
    super.beforeAll

    appConfig = ApplicationManager.getConfig()
    workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")

    // set up ZooKeeper Server
    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))
    ApplicationManager.updateWorkflowTime(System.currentTimeMillis())
  }

  test("batchHandler workflow") {
    val source2 = sc.textFile(path2).map(_.split(",")).map(x => Row.fromSeq(x.toSeq))
    val df1 = SQLContext.getOrCreate(sc).read.parquet(path1)

    assert(df1.columns.size == 7)

    val df2 = DataValidator(sc).applySchema(
      source2, ValidationConfig.getValidationConfig(appConfig , workflowConfig, "source2"))

    val batches =
      SeqSourceGenerator(
        workflowConfig,
        appConfig,
        sc,
        Seq(Map("source1" -> df1, "source2" -> df2)))
    BatchHandler.scheduleBatchRun(workflowConfig, batches, appConfig, sc)
  }

  override def afterAll(): Unit = {

    // Close ZooKeeper connections
    ZooKeeperConnection.close

    if (zk != null) {
      zk.shutdown()
      zk = null
    }
    super.afterAll
  }

}
