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
package org.apache.spark.util

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

/**
 * Created by Pankaj on 6/3/16.
 */
object TestUtils {

  val logger = LoggerFactory.getLogger(this.getClass)

  def getSparkConf(appConfig: ApplicationConfig = ApplicationManager.getConfig()): SparkConf = {

    val sparkConfigParam: Config = appConfig.sparkConfParam
    val sparkConf = new SparkConf
    sparkConf.setAppName(appConfig.appName)

    if (!sparkConfigParam.isEmpty) {
      val keyValueItr = sparkConfigParam.entrySet().iterator()
      while (keyValueItr.hasNext) {
        val sparkConfParam = keyValueItr.next()
        sparkConf.set(sparkConfParam.getKey, sparkConfParam.getValue.unwrapped().toString)
        logger.info(s"${sparkConfParam.getKey}: ${sparkConfParam.getValue.unwrapped().toString}")
      }
    }
    if (appConfig.env == "local") sparkConf.setMaster("local[2]")

    sparkConf
  }
}
