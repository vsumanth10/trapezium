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

// scalastyle:off

import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer
// scalastyle:on
import com.typesafe.config.Config
import org.apache.spark.sql.types._

/**
  * @author Hutashan
 * This class is for schema from conf file.
 * Checks number of column with number of datatype mentioned in config files
 *
 */
object SchemaBuilder {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Checks number of column with number of datatype mentioned in config files
   * @return Boolean
   */
  def matchNumberOfColAndDatatype(config: Config): Boolean = {
    config.getList(Constants.Column).size() == config.getList(Constants.DataType).size()
  }

  /**
   * Build schema from config file
   * @return StructType
   */

  def buildSchema(config: Config): StructType = {
    if (!matchNumberOfColAndDatatype(config)) {
      logger.error("Number of column and data type not matched", "")
      logger.error("Please check either column or datatype", "")
      throw new Exception("Number of column and Number of data type not matched")
    }
    val column = config.getStringList(Constants.Column)
    val datatype = config.getStringList(Constants.DataType)
    val datatypes = datatype.map(x => x.toLowerCase).map {
      case Constants.StringType => StringType
      case Constants.IntegerType => IntegerType
      case Constants.LongType => LongType
      case Constants.DoubleType => DoubleType
      case Constants.FloatType => FloatType
      case Constants.BinaryType => ByteType
      case Constants.ShortType => ShortType
      case Constants.DateType => DateType
      case Constants.BooleanType => BooleanType
      case Constants.TimestampType => TimestampType
      case _ => StringType
    }

    StructType((0 to column.size() - 1).map(x =>
      StructField(column.get(x), datatypes(x), true)).toArray)
  }
}
