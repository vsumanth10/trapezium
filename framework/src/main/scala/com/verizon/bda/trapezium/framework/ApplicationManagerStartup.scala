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

import java.io.{File, FileInputStream}
import java.util.Properties
import com.typesafe.config.{ConfigFactory, Config}
import com.verizon.bda.trapezium.framework.utils.EmptyRegistrator
import org.slf4j.LoggerFactory

/**
 * @author Pankaj on 10/30/15.
 *         debasish83 common getProperties to read Java/Typesafe properties
 */
trait ApplicationManagerStartup {

  val logger = LoggerFactory.getLogger(this.getClass)

  def init(envMgr: String,
           configDirMgr: String,
           persistSchemaMgr: String)

  def readProperties(configDir: String, propertyFile: String): Properties = {
    val properties = new Properties()
    if (configDir == null) {
      logger.info(s"Reading property file ${propertyFile} from jar")
      properties.load(classOf[EmptyRegistrator].getClassLoader().getResourceAsStream(propertyFile))
    } else {
      logger.info(s"Reading property file ${propertyFile} from ${configDir}")
      properties.load(new FileInputStream(s"${configDir}/$propertyFile"))
    }
    properties
  }

  def readConfigs(configDir: String, configFile: String): Config = {
    if (configDir == null) {
      logger.info(s"Reading config file ${configFile} from jar")
      ConfigFactory.load(configFile)
    } else {
      logger.info(s"Reading config file ${configFile} from ${configDir}")
      ConfigFactory.parseFile(new File(s"${configDir}/$configFile"))
    }
  }
}
