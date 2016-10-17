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
  * @author hutashan test wild char
  */
class ApplicationManagerWildCharSuite extends ApplicationManagerTestSuite {
 val startTime = System.currentTimeMillis()-500000

 override def beforeAll(): Unit = {
    super.beforeAll()
    FileCopy.fileDelete
    FileCopy.copyFiles(5, "special")
  }

  test("testDataSplitFiles workflow should successfully run the batch workflow") {
    ApplicationManager.updateWorkflowTime(startTime, "wildcharWorkFlow")
    val workFlowToRun = ApplicationManager.setWorkflowConfig("wildcharWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(sc)
  }


  test("file split with wild char workflow should successfully run the batch workflow") {
    val workFlowToRun = ApplicationManager.setWorkflowConfig("fileSplitSpecialCharWorkFlow")
    ApplicationManager.updateWorkflowTime(startTime, "fileSplitSpecialCharWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(sc)
  }

  test("file split with time stamp wild char workflow should successfully run the batch workflow") {

    val workFlowToRun = ApplicationManager.setWorkflowConfig("fileSplittimestampWorkFlow")
    ApplicationManager.updateWorkflowTime(startTime, "fileSplittimestampWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(sc)
  }

  override def afterAll(): Unit = {
    // Delete the temp directory
    FileCopy.fileDelete

    super.afterAll()

  }


}
