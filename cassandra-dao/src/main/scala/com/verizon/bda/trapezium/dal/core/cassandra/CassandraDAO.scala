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
package com.verizon.bda.trapezium.dal.core.cassandra

/**
  * Created by Faraz Waseem on 2/4/16.
  */
import java.util
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.{Select, QueryBuilder}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by v468328 on 1/25/16.
  * CassandraDAO provides high level scala interface to Cassandra.
  * It mantains list of sessions for each combination of (cluster + keyspace)
  * Calling progarm needs to call session.shutdown() once it don't need DAO
  * It is configurable by setConfigs
  */
@SerialVersionUID(220L)
class CassandraDAO(hosts: mutable.Buffer[String], dbName: String, tableName: String,
                   cassandraConfig: CassandraConfig =
                   new CassandraConfig(new util.HashMap[String, Object]()))
  extends Serializable {
  def this( hosts: java.util.List[String], dbName: String, tableName: String
          ){
    this(hosts.asScala, dbName,
      tableName)
  }
  private val columns: java.util.List[ColumnMetadata] = init()

  /**
    * Return CQL schema of table represented by this DAO.
    * @return
    */
  def getSchema(): Array[ColumnMetadata] = {
    columns.asScala.toArray
  }

  /**
    * This is general purpose select which gives pointer to (schmea + table ).
    * It is lazy and unless we perform an operation it don't execute anything
    * @return
    */

  def getAll(): Select = {
    QueryBuilder.select().from(dbName, tableName)
  }

  /**
    * This takes List of rows. Internal List[Object] translates to Cassandra Row
    * It also takes ttl. We can choose to have different TTL
    * if we use different writes using this method.
    * @param data
    */
  def write(data: List[List[Object]]): Unit = {
    // C* client driver's Insert doesn't seem to support bulk insert
    // need to handle explicitly here
    // using Async execution without batching
    // for ttl see if there is options set and it has TTL, if it is not there we will set it to 0
    var ttl_timeout: Int = 0;
    val ttl: Integer = cassandraConfig.configOptions.get("ttl").asInstanceOf[Integer]
    if (ttl != null) ttl_timeout = ttl.intValue()
    val session: Session = SessionManager.getSession(hosts, dbName, cassandraConfig);
    val statement: PreparedStatement = session.prepare(generateInsertStatment()
      + addTLL(ttl_timeout))
    val futures = new ListBuffer[ResultSetFuture]()
    for (oneRow <- data) {
      val bind = statement.bind(oneRow: _*)
      val resultSetFuture = session.executeAsync(bind)
      futures += resultSetFuture
    }
    // wait for inserts to finish
    for (future <- futures) {
      future.getUninterruptibly
    }
  }

  /**
    * This takes List of rows. Internal List[Object] translates to Cassandra Row
    * It execute the insert in batch mode
    * @param data
    */
  def writeBatch(data: List[List[Object]]): Unit = {
    writeBatch(data, 0)
  }

  /**
    * This takes List of rows. Internal List[Object] translates to Cassandra Row
    * It does it in batch mode
    * It also takes ttl (time to live for table)
    * @param data
    */
  def writeBatch(data: List[List[Object]], ttl_timeout: Int): Unit = {
    // C* client driver's Insert doesn't seem to support bulk insert
    val session: Session = SessionManager.getSession(hosts, dbName)
    val statement: PreparedStatement =
      session.prepare(generateInsertStatment() + addTLL(ttl_timeout))
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)

    batch.setConsistencyLevel(ConsistencyLevel.ONE)

    for (oneRow <- data) {
      val bind = statement.bind(oneRow: _*)
      batch.add(bind)
    }
    session.execute(batch)
  }

  /**
    * This is general purpose select which gives pointer to (schmea + table ).
    * It is lazy and unless we perform an operation it don't execute anything
    * It also filters out results by filter
    * @return
    */
  def getColumns(cols: mutable.Buffer[String]): Select = {
    QueryBuilder.select(cols: _*).from(dbName, tableName)
  }

  /**
    * Execute CQL statement. It is low level api
    * @param statement
    * @return
    */

  def execute(statement: Statement): ResultSet = {
    val session: Session = SessionManager.getSession(hosts, dbName );
    session.execute(statement)
  }

  private def addTLL(ttl_timeout: Int): String = {

    if (ttl_timeout > 0) {
      return " USING TTL " + ttl_timeout;
    }
    else return "";

  }


  /**
    * Initialize DAO. Assign it a session from SessionManager
    * @return
    */
  private def init(): java.util.List[ColumnMetadata] = {
    val session: Session = SessionManager.getSession(hosts, dbName, cassandraConfig)
    session.getCluster().getMetadata.getKeyspace(dbName).getTable(tableName).getColumns
  }

  private def generateInsertStatment(): String = {
    val sb = new StringBuilder("INSERT INTO ")

    sb.append(dbName).append('.').append(tableName).append(" (")
    // generate column name list
    var first = true
    val walker: java.util.Iterator[ColumnMetadata] = columns.iterator();
    while (walker.hasNext) {
      val column: ColumnMetadata = walker.next();
      if (!first) {
        sb.append(',')
      }
      else {
        first = false
      }
      sb.append(column.getName);
    }

    val walker2: java.util.Iterator[ColumnMetadata] = columns.iterator();

    // generate binding parameter foreach column

    sb.append(") VALUES (")
    first = true


    while (walker2.hasNext()) {
      walker2.next(); // move cursor one position
      if (!first) {
        sb.append(',')
      }
      else {
        first = false
      }
      sb.append('?')
    }
    sb.append(')')

    sb.toString()
  }


}


