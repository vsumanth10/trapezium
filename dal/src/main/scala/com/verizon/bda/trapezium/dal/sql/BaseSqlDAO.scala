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
/**
 * Copyright (C) Verizon Corp.
 */
package com.verizon.bda.trapezium.dal.sql

import com.verizon.bda.trapezium.dal.data.Data
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Row, SQLContext, DataFrame }

/**
 * @author Ken on 11/14/15.
 *         debasish83 17 Sept, 2016 BaseSqlDAO serializable for HiveDAO, CassandraDAO
 *         and LuceneDAO can be used inside DStream transformations
 */
abstract class BaseSqlDAO(dbName: String, tableName: String)(implicit sqlContext: SQLContext)
    extends Data[DataFrame] with Serializable {
  require(null != dbName && null != tableName && null != sqlContext)

  /**
   * This function creates a data frame according to the schema from the
   * given <code>tableName</code>.
   *
   * @param data <code>RDD[Row]</code> will be wrapped in <code>DataFrame</code>
   */
  final def createDataFrame(data: RDD[Row]): DataFrame = {
    sqlContext.createDataFrame(data, getSchema)
  }

  /**
   * The schema corresponding to the table <code>tableName</code> is returned.
   *
   * @return <code>StructType</code> containing the schema.
   */
  def getSchema: StructType

  protected def constructSql(cols: List[String]): String = {
    val colsLength = cols.length
    require(colsLength > 0, "Select at least one column to return.")
    var i = 0
    val fieldNames = getAll().columns
    while (i < colsLength) {
      require(fieldNames.contains(cols(i)),
        s"All column names should be present in the input table: ${cols(i)}")
      i += 1
    }

    s"select ${cols.mkString(",")} from $tableName"
  }
}
