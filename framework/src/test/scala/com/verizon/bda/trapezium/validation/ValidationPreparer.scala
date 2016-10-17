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
package com.verizon.bda.trapezium.validation

import com.typesafe.config.{ConfigObject, Config, ConfigFactory}
import com.verizon.bda.trapezium.framework.ApplicationManager


object ValidationPreparer extends Serializable {

  def getValidationConfig(): Config = {
    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    val inputName = "source1"
    ValidationConfig.getValidationConfig(appConfig , workflowConfig, inputName)
  }

  def getValidationConfigSources2(): Config = {
    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    val inputName = "source2"
    ValidationConfig.getValidationConfig(appConfig, workflowConfig, inputName)
  }

  def getBadValidationConfig(): Config = {
    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig("badValidation")

    workflowConfig.workflowConfig.asInstanceOf[Config]
  }

  def getConfigFileSplit(workflow : String) : Config = {
    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflow)

    val inputName = "testDataSplitFiles"
    ValidationConfig.getSplitFile(appConfig, workflowConfig, inputName)
    //    .asInstanceOf[Config].getConfig("splitfile")

  }

  def getValidationConfig(
                           workflow : String,
                           inputName : String) : Config = {
    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflow)
    ValidationConfig.getValidationConfig(appConfig , workflowConfig, inputName)

  }
}
