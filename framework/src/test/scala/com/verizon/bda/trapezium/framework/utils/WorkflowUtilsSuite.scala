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
package com.verizon.bda.trapezium.framework.utils

import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.spark.Logging
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by Pankaj on 7/12/16.
 */
class WorkflowUtilsSuite extends FunSuite with BeforeAndAfterAll with Logging {

  var zk: EmbeddedZookeeper = _
  val appConfig = ApplicationManager.getConfig()

  override def beforeAll {

    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))
  }

  override def afterAll {

    ZooKeeperConnection.close

    if (zk != null) {
      zk.shutdown()
    }

  }

  test("Reset workflow timestamp"){

    val workflowName = "testWorkFlow1"
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)

    val currentTime = System.currentTimeMillis
    ApplicationUtils.updateCurrentWorkflowTime(workflowName, currentTime, appConfig.zookeeperList)

    val args = Array("--workflow", workflowName, "--workflowTime", "0", "--action", "set")
    WorkflowUtils.main(args)

    val currentWorkflowTime = ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig)

    assert (currentWorkflowTime == 0)

  }

  test("Get workflow timestamp"){

    val workflowName = "testWorkFlow1"
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)

    val currentTime = System.currentTimeMillis
    ApplicationUtils.updateCurrentWorkflowTime(workflowName, currentTime, appConfig.zookeeperList)

    val args = Array("--workflow", workflowName, "--action", "get")
    WorkflowUtils.main(args)

    val currentWorkflowTime = ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig)

    assert (currentWorkflowTime == currentTime)

  }

}
