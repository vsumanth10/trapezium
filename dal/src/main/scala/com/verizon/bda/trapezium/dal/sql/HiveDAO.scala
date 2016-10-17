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

import com.verizon.bda.trapezium.dal.exceptions.HiveDAOException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

/**
 * Read/Write data from/to hive table. Here the SQLContext is instantiated
 * using the <code>SparkContext</code> object passed in implicitly.
 *
 * @author pramod.lakshminarasimha
 *
 * @param tableName name of the hive table to connect.
 */
class HiveDAO(database: String, tableName: String)(implicit sqlContext: HiveContext)
    extends BaseSqlDAO(database, tableName) {

  require(database != null && tableName != null)

  private def tableExists = sqlContext.tableNames(database).contains(tableName)

  /**
   * The schema corresponding to the table <code>tableName</code> is returned.
   *
   * @return <code>StructType</code> containing the schema.
   */
  override def getSchema: StructType = sqlContext.table(tableName).schema

  /**
   * All the columns from hive table <code>tableName</code>
   * are returned as a <code>DataFrame</code>.
   *
   * @return <code>DataFrame</code> containing all columns.
   */
  final def getAll: DataFrame = {
    if (!tableExists) {
      throw new HiveDAOException(s"Table ${tableName} does not exist")
    }
    sqlContext.table(tableName)
  }

  /**
   * The selected columns from hive table <code>tableName</code>
   * are returned as a <code>DataFrame</code>.
   *
   * @return <code>DataFrame</code> containing the selected columns <code>cols</code>.
   */
  final def getColumns(cols: List[String]): DataFrame = {
    if (!tableExists) {
      throw new HiveDAOException(s"Table ${tableName} does not exist")
    }
    val sqlText = constructSql(cols)
    log.info(s"Executing query: ${sqlText}")
    sqlContext.sql(sqlText)
  }

  /**
   * This function overwrites the data frame into hive table.
   * If the table <code>tableName</code> does not exists, this throws an exception.
   *
   * @param df <code>DataFrame</code> that will be persisted into
   * hive table <code>tableName</code>
   */
  override final def write(df: DataFrame): Unit = {
    validateSchema(df)
    if (!tableExists) {
      throw new HiveDAOException(s"Table ${tableName} does not exist.")
    }
    switchContext(df).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  /**
   * This function inserts the Rows of the DataFrame <code>df</code> into
   * the hive table <code>tableName</code>.
   *
   * @param df <code>DataFrame</code> that will be persisted into
   * hive table <code>tableName</code>
   * @param partitions Sequence of partition column names [eg. {{{write(df, Seq("date"))}}}].
   */
  final def write(df: DataFrame, partitions: Seq[String]): Unit = {
    validateSchema(df)
    if (!tableExists) {
      throw new HiveDAOException(s"Table ${tableName} does not exist")
    }
    val dfPartitioned = switchContext(df).write.partitionBy(partitions: _*)
    dfPartitioned.insertInto(tableName)
  }

  private def switchContext(df: DataFrame): DataFrame = {
    if (df.sqlContext == this.sqlContext) {
      df
    } else {
      createDataFrame(df.rdd)
    }
  }

  /**
   * Here we check if the name and datatype of the hive table matches with
   * the data frame passed in. The parameter <code>nullable</code> in StructField is not
   * persisted in hive table properties. Hence it cannot be checked.
   *
   * @param df DataFrame to be tested
   */
  private def validateSchema(df: DataFrame): Unit = {
    val dfFields = df.schema.fields.toList
    val tableFields = sqlContext.table(tableName).schema.fields.toList
    require((dfFields.map(_.name) == tableFields.map(_.name)) &&
      (dfFields.map(_.dataType) == tableFields.map(_.dataType)),
      s"""Schema of dataframe does not match with table schema:
      |DataFrame schema: ${df.schema} \n
      |Table schema: ${sqlContext.table(tableName).schema}""".stripMargin)
  }

  /**
   * This function deletes the specified partitions in the hive table.
   *
   *  @param partitionSpec specify partitions (eg. "date = '2015-10-31'")
   */
  final def deletePartition(partitionSpec: String): Unit = {
    if (!tableExists) {
      throw new HiveDAOException(s"Table ${tableName} does not exist")
    }
    sqlContext.sql(s"ALTER TABLE ${tableName} DROP PARTITION (${partitionSpec})")
  }

  /**
   * This function deletes all the partitions specified in the argument partitionSpecs.
   * It calls <code>deletePartition</code> function for each
   * element in the array <code>partitionSpecs</code>.
   *
   *  @param partitionSpecs specify partitions (eg. Array("date = '2015-10-31'"))
   */
  final def deletePartitions(partitionSpecs: Array[String]): Unit = {
    if (!tableExists) {
      throw new HiveDAOException(s"Table ${tableName} does not exist")
    }
    partitionSpecs.foreach { spec => deletePartition(spec) }
  }
}
