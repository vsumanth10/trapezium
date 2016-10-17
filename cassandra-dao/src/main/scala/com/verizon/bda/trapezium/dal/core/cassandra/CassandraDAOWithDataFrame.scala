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
package com.verizon.bda.trapezium.dal.core.cassandra.utils

import com.verizon.bda.trapezium.dal.core.cassandra.{CassandraConfig, CassandraDAO}
import org.apache.spark.sql.{Row, DataFrame}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.collection.mutable
/**
  * @author v468328 on 2/29/16.
  *         debasish83 column name mapping from dataframe to cassandra
  *         TODO: Add support for SparkSQL ArrayType and MapType
  */
class CassandraDAOWithDataFrame(val hostList: mutable.Buffer[String],
                                val schema: String,
                                val table: String) extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  def getColumns(basicDAO: CassandraDAO) : Array[String] = {
    val columns = basicDAO.getSchema().map(_.getName)
    logger.info(s"core dao columns ${columns.mkString(",")}")
    columns
  }

  def persistWithDataFrameToDaoAdapter(dataFrame: DataFrame, config: CassandraConfig): Unit = {
    dataFrame.foreachPartition(x => {
      val basicDAO = new CassandraDAO(hostList, schema, table)
      val columns = getColumns(basicDAO)
      persistRecords(basicDAO, x, columns, config)
    })
  }

  def persistWithDataFrameToDaoAdapter(dataFrame: DataFrame): Unit = {
    dataFrame.foreachPartition(x => {
      val basicDAO = new CassandraDAO(hostList, schema, table)
      val columns = getColumns(basicDAO)
      persistRecords(basicDAO, x, columns)
    })
  }

  def transformToList(row: Row, columns: Array[String]): List[Object] = {
    val rowList = new ListBuffer[Object]
    var i = 0
    while (i < columns.size) {
      var item = row.getAs[Object](columns(i))
      rowList += item
      i += 1
    }
    return rowList.toList
  }

  def persistRecords(basicDAO: CassandraDAO,
                     rowWalker: Iterator[Row],
                     columns: Array[String]): Unit = {
    logger.info("reached inside persist record after creating DAO")
    val rowListWalker = rowWalker.map(x => transformToList(x, columns))
    basicDAO.write(rowListWalker.toList)
  }

  def persistRecords(basicDAO: CassandraDAO,
                     rowWalker: Iterator[Row],
                     columns: Array[String],
                     options: CassandraConfig): Unit = {
    logger.info("reached inside persist record with configs")
    val rowListWalker = rowWalker.map(x => transformToList(x, columns))
    val basicDAO = new CassandraDAO(hostList, schema, table)
    basicDAO.write(rowListWalker.toList)
  }
}
