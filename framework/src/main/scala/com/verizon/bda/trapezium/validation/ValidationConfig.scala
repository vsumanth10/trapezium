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

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigObject}
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * Created by Hutashan on 2/29/16.
 */
object ValidationConfig {

  def getValidationConfig(appConfig : ApplicationConfig,
                          workflowConfig: WorkflowConfig,
                          sourceName : String) : Config = {

    val runMode = workflowConfig.runMode
    val dataSource = workflowConfig.dataSource

    val sourceConfigList: ConfigList = runMode match {
      case "STREAM" => {
        if (dataSource == "HDFS") {
          workflowConfig.hdfsStream.asInstanceOf[Config].getList("streamsInfo")
        }
        else if (dataSource == "KAFKA") {
          workflowConfig.kafkaTopicInfo.asInstanceOf[Config].getList("streamsInfo")
        }
        else {
          null
        }
      }
      case "BATCH" => {
        if (dataSource == "HDFS") {
          workflowConfig.hdfsFileBatch.asInstanceOf[Config].getList("batchInfo")
        }
        else if (dataSource == "KAFKA") {
          workflowConfig.kafkaTopicInfo.asInstanceOf[Config].getList("streamsInfo")
        }
        else {
          null
        }
      }
      case _ => null
    }

      var sourceConfig: Config = null
      sourceConfigList.asScala.foreach { conf => {
        val sourceData = conf.asInstanceOf[ConfigObject].toConfig
        val source = sourceData.getString("name")
        if (source.equals(sourceName)) {
          try {
            sourceConfig = sourceData.getConfig("validation")
          }
          catch {
            case _: Throwable => sourceConfig = ConfigFactory.empty()
          }
        }
      }
    }
    sourceConfig
  }

  def getSplitFile(appConfig : ApplicationConfig,
                          workflowConfig: WorkflowConfig,
                          sourceName : String) : Config = {

    val runMode = workflowConfig.runMode
    val dataSource = workflowConfig.dataSource

    val sourceConfigList: ConfigList = runMode match {
      case "STREAM" => {
        if (dataSource == "HDFS") {
          workflowConfig.hdfsStream.asInstanceOf[Config].getList("streamsInfo")
        }
        else if (dataSource == "KAFKA") {
          workflowConfig.kafkaTopicInfo.asInstanceOf[Config].getList("streamsInfo")
        }
        else {
          null
        }
      }
      case "BATCH" => {
        if (dataSource == "HDFS") {
          workflowConfig.hdfsFileBatch.asInstanceOf[Config].getList("batchInfo")
        }
        else if (dataSource == "KAFKA") {
          workflowConfig.kafkaTopicInfo.asInstanceOf[Config].getList("streamsInfo")
        }
        else {
          null
        }
      }
      case _ => null
    }

    var sourceConfig: Config = null
    sourceConfigList.asScala.foreach { conf => {
      val sourceData = conf.asInstanceOf[ConfigObject].toConfig
      val source = sourceData.getString("name")
      if (source.equals(sourceName)) {
        try {
          sourceConfig = sourceData.getConfig("splitfile")
        }
        catch {
          case _: Throwable => sourceConfig = ConfigFactory.empty()
        }
      }
    }
    }
    sourceConfig
  }





}
