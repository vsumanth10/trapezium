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
package com.verizon.bda.trapezium.dal.spark.cassandra

import com.verizon.bda.trapezium.dal.sql.BaseSqlDAO
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.types.StructType

/**
  * Created by Faraz Waseem on 12/17/15.
  * Cassandrao DAO is wrapper around spark connector which is high level interface to Cassandra
  * It has defaults options which can be overwritten in spark conf options.
  * like sc.conf.set("spark.cassandra.connection.host", "md-bdadev-54.verizon.com")
  *
  */
class CassandraDAO(dbName: String, tableName: String,
                  hosts: List[String]) (implicit sqlContext: CassandraSQLContext)
  extends BaseSqlDAO(dbName, tableName) {
  protected val schema = null;
  var dbMap = Map("table" -> tableName, "keyspace" -> dbName)
  /**
    * The schema corresponding to the table <code>tableName</code> is returned.
    *
    * @return <code>StructType</code> containing the schema.
    */
  override def getSchema: StructType = schema



  def this (dbName: String, tableName: String,
            hosts: List[String],
            options: Map[String, String]) (implicit sqlContext: CassandraSQLContext) {
    this(dbName, tableName, hosts)
    dbMap ++= options

  }

  /**
    * Write the batch of records (eg. Rows) into the data store.
    *
    * @param data distributed collection of data to be persisted of type <code>DataFrame</code>
    */
  override def write(data: DataFrame): Unit = {

   // data.write.insertInto(dbName + "." +  tableName)
    data.write
      .format("org.apache.spark.sql.cassandra")
      .options(dbMap)
      .mode(SaveMode.Append)
      .save()


  }

  /**
    * The requested list of columns are returned as batch.
    *
    * @param cols List of columns to return
    * @return T distributed collection of data containing the requested columns.
    */
  override def getColumns(cols: List[String]): DataFrame = {
    val sqlText = constructSql(cols)
    log.info(s"Executing query: $sqlText")

    val df = sqlContext.sql(sqlText)
    df
  }

  def getOptions(): Map[String, String] = {

    dbMap.toMap
  }

  /**
    * All the columns from source are returned as batch.
    *
    * @return T distributed collection of data containing all columns.
    */
  override def getAll(): DataFrame = {
    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(dbMap)
      .load()

    df
  }

  /**
    * It is helper method to create a host string from list like
    * @param hosts
    * @return
    */
  private def createStringList( hosts: List[String]): String = {

    val builder: StringBuilder = new StringBuilder
    hosts.foreach((i: String) => {
      builder.append(i); builder.append(" , ") })


   val hostString: String = builder.toString();
    return hostString.substring(0, hostString.lastIndexOf(","));

  }



}
