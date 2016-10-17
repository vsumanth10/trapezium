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

import java.text.SimpleDateFormat
import java.util.List
import au.com.bytecode.opencsv.CSVParser
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.LoggerFactory
import scala.collection.mutable.{Map => MMap}
// scalastyle:off
import scala.collection.JavaConversions.asScalaBuffer
// scalastyle:on
/**
 * @author Hutashan Data validation as per config with skipHeader logic
 *         debasish83 common validation for batch and stream mode
 *                    accumulators added for batch and stream mode
 *                    TO DO: Check if accumulator works in stream mode
 */

class DataValidator(sc: SparkContext) extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  var parser: CSVParser = _
  var delimiterLastTime: Char = ','

  val accRawTotalCount = sc.accumulator(0L)
  val accFinalCount = sc.accumulator(0L)

  def filterData(row: Row, column: List[String], dataType: List[String],
                 validation: Config, sFormat: SimpleDateFormat,
                 delimiter: Char, numberOfColumn: Int): Option[Seq[Any]] = {

    if (parser == null || !delimiterLastTime.equals(delimiter)) {
      parser = new CSVParser(delimiter)
      delimiterLastTime = delimiter
    }

    try {
      val arrLine = parser.parseLine(row.getString(0))
      if (ValidateColumn.validateNumberOfColumns(arrLine, numberOfColumn)) {
        Some(ValidateDataType.validate(arrLine, column, dataType, validation, sFormat, delimiter))
      } else {
        logger.warn("Expected records count failed. " +
          " Expected min number of column is " + numberOfColumn
          + ",  But actual size is :" + arrLine.length +
          " Line: " + row.getString(0))
        None
      }
    }
    catch {
      case ex: Throwable =>
        logger.warn("Error in parsing line." + row.getString(0)
          + ex.getStackTrace.take(200))
        None
    }
  }

  /**
   * Apply schema for data frames
   * @param rowFilter
   * @param config
   * @return dataframe
   */
  def applySchema(rowFilter: RDD[Row], config: Config): DataFrame = {
    require(rowFilter != null)
    val sqlContext = SQLContext.getOrCreate(rowFilter.context)
    val schema = SchemaBuilder.buildSchema(config)
    val inputDF = sqlContext.createDataFrame(rowFilter, schema)
    inputDF
  }

  /**
   * validate input raw line based in config
   * @param rows rdd of input raw lines
   * @param config config object
   * @return validated data
   */
  def validate(rows: RDD[Row], config: Config): RDD[Row] = {
    val numberOfCol = config.getString(Constants.MinNumberOfColumn).toInt
    val column = config.getStringList(Constants.Column)
    val dataType = config.getStringList(Constants.DataType)

    val validation = config.getConfig(Constants.Validation)
    val sFormat: SimpleDateFormat = new SimpleDateFormat(config.getString(Constants.DateFormat))
    val delimiter = config.getString(Constants.Delimiter).toCharArray
    // TO DO: add the skip header feature from spark-csv in cleaner way
    // TO DO: Expose accumulators through
    val rowsSkippedHeader = rows.filter { row =>
      try {
        accRawTotalCount.add(1);
        row.getString(0)!=null && row.getString(0).charAt(0) != '#'
      } catch {
        case ex: Throwable => logger.warn("Data is failing on charAt(0)")
          false
      }
    }

    val rowsHandleSpecialCharacter = removeSpecialCharIfAny(rowsSkippedHeader, config)
    val validated = rowsHandleSpecialCharacter.flatMap { x => filterData(x, column, dataType,
      validation, sFormat, delimiter(0), numberOfCol)
    }.map { x => accFinalCount.add(1); Row.fromSeq(x) }
    validated
  }

  /**
   * validate input raw line based in config
   * @param rows rdd of input raw lines
   * @param config config object
   * @return validated dataframe
   */
  def validateDf(rows: RDD[Row], config: Config): DataFrame = {
    applySchema(validate(rows, config), config)
  }

  def removeSpecialCharIfAny(rows: RDD[Row], config: Config): RDD[Row] = {
    if (config.hasPath("specialCharacterReplace")) {
      val specialCharRegex = config.getString("specialCharacterReplace")
      rows.map(row => Row(row.getString(0).replaceAll(specialCharRegex, "")))
    } else {
      rows
    }
  }
}

object DataValidator {
  val logger = LoggerFactory.getLogger(this.getClass)

  val dvMap = MMap[String, DataValidator]()

  def apply(sc: SparkContext): DataValidator = new DataValidator(sc)

  def apply(sc: SparkContext, sourceName: String): DataValidator = {
    val dv = dvMap.get(sourceName)
    if (dv != None) return dv.get
    this.synchronized {
      dvMap += sourceName -> DataValidator(sc)
      dvMap(sourceName)
    }
  }

  def resetAccumulators: Unit = {
    dvMap.foreach { case (sourceName, dv) =>
      dv.accRawTotalCount.value = 0
      dv.accFinalCount.value = 0
      logger.info(s"Resetting accumulator for input source ${sourceName}" +
        s"input row count: ${dv.accRawTotalCount.value}, " +
        s"good record count: ${dv.accFinalCount.value}")
    }

  }

  def printStats(): Unit = {
    dvMap.foreach { case (sourceName, dv) =>
      logger.info(s"Batch stats counts for input source ${sourceName} " +
        s"input row count: ${dv.accRawTotalCount.value}, " +
        s"good record count : ${dv.accFinalCount}")
    }
  }
}

