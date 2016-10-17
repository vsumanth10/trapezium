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

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
 * This is starting point of validator framework.
    initialize  properties of validator and set spark context
 */
object Validator {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * This is start point of validation framework
   * @param inputRow
   * @param config
   * @return
   */
  def startValidation(streamName: String,
                      inputRow: RDD[Row],
                      config: Config): DataFrame = {
    val validator = DataValidator(inputRow.sparkContext, streamName)
    validator.validateDf(inputRow, config)
  }

  def getValidatedStream(streamName: String,
                         dStreamRow: DStream[Row],
                         streamInfo: Config): DStream[Row] = {

    val fileFormat =
      if (streamInfo.hasPath("fileFormat")){
        streamInfo.getString("fileFormat")
      } else {
        "text"
      }

    fileFormat.toUpperCase match {
      case "PARQUET" => {
        logger.info(s"input source is Parquet")
        dStreamRow
      }
      case "AVRO" => {
        logger.info(s"input source is Avro")
        dStreamRow
      }
      case "JSON" => {
        logger.info(s"input source is Json")
        dStreamRow
      }
      case _ => {
        logger.info(s"input source is text")
        val validationConfig = streamInfo.getConfig("validation")
        val validatedDStream = validateStream(streamName, dStreamRow, validationConfig)

        validatedDStream
      }
    }
  }

  private def validateStream(streamName: String,
                       dstream: DStream[Row],
                       config: Config): DStream[Row] = {
    val validator = DataValidator(dstream.context.sparkContext, streamName)
    dstream.transform { (rows) => validator.validate(rows, config) }
  }

  def countTotal(streamName: String): Long = {
    DataValidator.dvMap(streamName).accRawTotalCount.value
  }

  def countFiltered(streamName: String): Long = {
    DataValidator.dvMap(streamName).accFinalCount.value
  }
}
