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
package com.verizon.bda.trapezium.framework.manager

import java.io.File

import com.typesafe.config.{ConfigList, ConfigObject, ConfigFactory, Config}
import com.verizon.bda.trapezium.framework.ApplicationManager
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * Created by Pankaj on 2/17/16.
 */
class WorkflowConfig
(val workflow: String) extends Serializable{

  val logger = LoggerFactory.getLogger(this.getClass)
  val workflowConfig: Config = {

    val appConfig = ApplicationManager.getConfig()

    logger.info(s"Config directory is ${appConfig.configDir}")
    if (appConfig.configDir != null) {

      val workflowConfigFilePath = s"${appConfig.configDir}/${workflow}.conf"
      val workflowConfigFile: File = new File(workflowConfigFilePath)
      ConfigFactory.parseFile(workflowConfigFile)
    } else {
      ConfigFactory.load(s"${workflow}.conf")
    }
  }.resolve()

  lazy val runMode = workflowConfig.getString("runMode")

  lazy val dataSource =
    try {

      workflowConfig.getString("dataSource")
    } catch {
      case ex: Throwable =>

        logger.error("Invalid config file", s"dataSource must be present")
        throw ex
    }

  lazy val syncWorkflow =
    try {
      workflowConfig.getString("syncWorkflow")
    } catch {
      case ex: Throwable => {
        logger.warn(s"Config property dependentWorkflow not defined." +
          s" This means ${workflow} does not depend on any other workflow.")
        null
      }
    }

  lazy val dependentWorkflows =
    try {
      workflowConfig.getConfig("dependentWorkflows").getStringList("workflows")
    } catch {
      case ex: Throwable => {
        logger.warn(s"Config property dependentWorkflows not defined." +
          s" This means ${workflow} does not depend on any other workflow.")
        null
      }
    }


  lazy val dependentFrequencyToCheck =
    try {
      workflowConfig.getConfig("dependentWorkflows").getLong("frequencyToCheck")
    } catch {
      case ex: Throwable => {
        logger.warn(s"Config property frequencyToCheck not defined." +
          s" This means ${workflow} does not depend on any other workflow.")
        60000L
      }
    }

  lazy val transactions =
    try {
      workflowConfig.getConfigList("transactions")
    } catch {
      case ex: Throwable =>

        logger.error("Invalid config file",
          s"At least one transaction must be specified for ${workflow}")
        throw ex
    }

  lazy val hdfsStream =
    try {
      workflowConfig.getConfig("hdfsStream")
    } catch {
      case ex: Throwable =>
        runMode match {
          case "STREAM" => {
            logger.error("Invalid config file", s"hdfsStream must be present for $runMode")
            throw ex
          }
          case _ => {

            logger.warn("Missing entry in config file",
              s"hdfsStream is not present. Ignoring as it is not required in $runMode")
          }
        }
    }

  lazy val kafkaTopicInfo =
    try {
      workflowConfig.getConfig("kafkaTopicInfo")
    } catch {
      case ex: Throwable =>
        dataSource match {
          case "KAFKA" => {
            logger.error("Invalid config file",
              s"kafkaTopicInfo must be present for $dataSource")
            throw ex
          }
          case _ => {

            logger.warn("Missing entry in config file",
              s"kafkaTopicInfo is not present. Ignoring as it is not required for ${dataSource}")
          }
        }
    }

  lazy val hdfsFileBatch =
    try {
      workflowConfig.getConfig("hdfsFileBatch")
    } catch {
      case ex: Throwable => {
        logger.warn("Missing entry in config file",
          s"hdfsFileBatch is not present.")
      }
    }

  lazy val httpServer =
    try {
      workflowConfig.getConfig("httpServer")
    } catch {
      case ex: Throwable => {
        logger.warn("Missing entry in config file",
          s"httpServer is not present.")
        null
      }
    }

  def fileFormat(inputName: String): String = {
    var fileFormat = "text"
    try {
      val batchInfoList: ConfigList = hdfsFileBatch.asInstanceOf[Config].getList("batchInfo")
      batchInfoList.asScala.foreach { batchConfig =>
        val batchData = batchConfig.asInstanceOf[ConfigObject].toConfig
        val name = batchData.getString("name")

        if(name.equals(inputName)){

          fileFormat = batchData.getString("fileFormat")
        }
      }
    } catch {

      case ex: Throwable => {

        logger.warn(s"No file format present. Using default text")

      }
    }
    fileFormat
  }
}
