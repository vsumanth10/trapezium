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
import com.typesafe.config.{Config, ConfigFactory}
import kafka.common.TopicAndPartition
import org.slf4j.LoggerFactory

/**
 * @author Pankaj on 10/21/15.
 *         debasish83 modified to create instances of ApplicationConfig for
 *         ApplicationManager.getConfig API
 */
class ApplicationConfig
(val env: String, val configDir: String) extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  private val config: Config = if (configDir != null) {
    val configFilePath = s"${configDir}/${env}_app_mgr.conf"
    val configFile: File = new File(configFilePath)
    ConfigFactory.parseFile(configFile)
  } else {
    ConfigFactory.load(s"${env}_app_mgr.conf")
  }

  if (configDir != null) {
    logger.info(s"Reading Config File location ${configDir}/${env}_app_mgr.conf")
  } else {
    logger.info(s"Reading ${env}_app_mgr.conf from jar")
  }

  lazy val appName =
    try {
      config.getString("appName")
    } catch {
      case ex: Throwable =>
        logger.warn("Config property appName not defined. Using default value.")
        "ApplicationManager"
    }

  lazy val persistSchema =
    try {
      config.getString("persistSchema")
    } catch {
      case ex: Throwable => {
        logger.error("Invalid config file", s"appSchema must be present in conf file")
        throw ex
      }
    }

  lazy val tempDir = config.getString("tempDir")

  lazy val executorPath =
    try {
      config.getString("executorPath")
    } catch {
      case ex: Throwable =>
        logger.warn("Config property executorPath not defined. Using default value.")
        ""
    }

  lazy val executorLibraryPath =
    try {
      config.getString("executorLibraryPath")
    } catch {
      case ex: Throwable =>
        logger.warn("Config property executorLibraryPath not defined. Using default value.")
        ""
    }

  lazy val zookeeperList = config.getString("zookeeperList")

  lazy val kafkabrokerList = config.getString("kafkabrokers")

  lazy val applicationStartupClass = config.getString("applicationStartupClass")

  lazy val fileSystemPrefix = config.getString("fileSystemPrefix")

  val streamtopicpartionoffset =
    new collection.mutable.HashMap[String, Map[TopicAndPartition, (Long, Long)]]()


  lazy val sparkConfParam =
    try {
      config.getConfig("sparkConf")
    } catch {

      case ex: Throwable => {
        logger.warn("Config property sparkConf not defined, spark conf properties will be blank")
      }
        ConfigFactory.empty
    }

  lazy val kafkaConfParam =
    try {
      config.getConfig("kafkaConf")
    } catch {
      case ex: Throwable => {
        logger.warn("Config property kafkaConf not defined, kafka conf properties will be blank")
      }
        ConfigFactory.empty
    }
}
