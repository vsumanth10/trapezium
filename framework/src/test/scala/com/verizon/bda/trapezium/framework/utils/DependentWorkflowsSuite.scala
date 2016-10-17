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

import com.verizon.bda.trapezium.framework.{ApplicationManager, ApplicationManagerTestSuite}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory


class DependentWorkflowsSuite extends ApplicationManagerTestSuite {

  val logger = LoggerFactory.getLogger(this.getClass)

  test("dependent work flow test") {
    val workflowConfig = ApplicationManager.setWorkflowConfig("dependentWorkflow")
    val dependentTime = ApplicationUtils.getDependentsWorkflowTime(
      ApplicationManager.getConfig(), workflowConfig)
    assert(dependentTime.size == 2)
  }


  test("dependent workflow executed." +
    " Current workflow should run isDependentWorkflowExecuted function should return true") {
    ApplicationManager.updateWorkflowTime(System.currentTimeMillis() , "dependentWorkflow")

    ApplicationManager.updateWorkflowTime(
      System.currentTimeMillis() + 10000 , "dependent1")
    ApplicationManager.updateWorkflowTime(
      System.currentTimeMillis() + 10000, "dependent2")
    val workflowConfig = ApplicationManager.setWorkflowConfig("dependentWorkflow")
    val dependentTime = ApplicationUtils.isDependentWorkflowExecuted(
      ApplicationManager.getConfig(), workflowConfig)
    assert(dependentTime)
  }

  test("one of dependent workflow executed. " +
    "Current workflow should not run isDependentWorkflowExecuted function should return false") {
    val currentTime = System.currentTimeMillis()
    ApplicationManager.updateWorkflowTime(currentTime, "dependent1")
    ApplicationManager.updateWorkflowTime(currentTime - 5000, "dependent2")
    ApplicationManager.updateWorkflowTime(currentTime, "dependentWorkflow")
    val workflowConfig = ApplicationManager.setWorkflowConfig("dependentWorkflow")
    val dependentTime = ApplicationUtils.isDependentWorkflowExecuted(
      ApplicationManager.getConfig(), workflowConfig)
    assert(dependentTime == false)
  }


  test("No dependency. should return true") {
    val workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    val dependentTime = ApplicationUtils.isDependentWorkflowExecuted(
      ApplicationManager.getConfig(), workflowConfig)
    assert(dependentTime)
  }



  test("Test dependency waitForDependentWorkflow") {
    val dependentTest = new WFThread(sc, "dependentWorkflow")
    dependentTest.start()
    Thread.sleep(70000)

    // Run workflow1
    val workflowConfig1 = ApplicationManager.setWorkflowConfig("dependent1")
    ApplicationManager.runBatchWorkFlow(
      workflowConfig1, ApplicationManager.getConfig(), 1)(sc)

    // Run workflow2
    val workflowConfig2 = ApplicationManager.setWorkflowConfig("dependent2")
    ApplicationManager.runBatchWorkFlow(
      workflowConfig2, ApplicationManager.getConfig(), 1)(sc)

    logger.info("dependent val set")
    while (dependentTest.isRunning) {
      Thread.sleep(100)
    }
  }




}

class WorkflowThread (sc : SparkContext) extends Thread {
  var isRunning = true
  override def run(): Unit = {
    val workflowConfig = ApplicationManager.setWorkflowConfig("dependentWorkflow")
    ApplicationManager.runBatchWorkFlow(
      workflowConfig, ApplicationManager.getConfig(), 1)(sc)
    isRunning = false
  }
}

class WFThread (sc : SparkContext, wf : String) extends Thread {
  var isRunning = true
  override def run(): Unit = {
    val workflowConfig = ApplicationManager.setWorkflowConfig(wf)
    ApplicationManager.runBatchWorkFlow(
      workflowConfig, ApplicationManager.getConfig(), 1)(sc)
    isRunning = false
  }
}
