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

import java.util.Calendar

import com.verizon.bda.trapezium.framework.handler.FileCopy
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils

/**
  * @author hutashan test file split
  */
class ApplicationManagerFileSplitSuite extends ApplicationManagerTestSuite {

  var startTime = System.currentTimeMillis()-500000
  override def beforeAll(): Unit = {
    super.beforeAll()
    FileCopy.fileDelete
    FileCopy.copyFiles(2)
  }

  test("testDataSplitFiles workflow should successfully run the batch workflow") {
    ApplicationManager.updateWorkflowTime(startTime, "fileSplitWorkFlow")
    val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("fileSplitWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig , maxIters = 1)(sc)
  }

  test("testDataSplitFiles ") {
    ApplicationManager.updateWorkflowTime(startTime, "fileSplitWorkFlow")
    var cal = Calendar.getInstance()
    cal.add(Calendar.DATE, - 1)
    val processedDate = getStartOfDay(cal.getTime).getTime
    val keyFileProcessed = s"/etl/fileSplitWorkFlow/fileProccessed"
    ApplicationUtils.updateZookeeperValue(keyFileProcessed,
      processedDate, appConfig.zookeeperList)
    val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("fileSplitWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(sc)
  }

  def getStartOfDay(dt : java.util.Date) : java.util.Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(dt)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    return calendar.getTime()
  }

  override def afterAll(): Unit = {
    // Delete the temp directory
    FileCopy.fileDelete

    super.afterAll()

  }


}
