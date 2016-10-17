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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
/**
  * Created by v468328 on 2/29/16.
  */
class CassandraDAOUtils(val hostList: ListBuffer[String],
                        val schmea: String, val table: String) extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  val daoTestSchema = StructType(
    Seq(StructField("ipaddress", LongType, true),
      StructField("processdate", DateType, true),
      StructField("origipaddress", StringType, true),
      StructField("color", StringType, true),
      StructField("description", StringType, true),
      StructField("reason", StringType, true),
      StructField("notes", StringType, true)));

  val daoTestSchema2 = StructType(
    Seq(StructField("ipaddress", LongType, true),
      StructField("origipaddress", StringType, true),
      StructField("color", StringType, true),
      StructField("description", StringType, true),
      StructField("reason", StringType, true),
      StructField("processdate", DateType, true),
      StructField("notes", StringType, true)));


  def persistWithDataFrameToDaoAdapterWithTTL(dataFrame: DataFrame, ttl: Int): Unit = {

    // first we need RDD from dataframe
    dataFrame.foreachPartition(x => persistRecordsWithTTL(x, ttl));

  }

  def persistWithDataFrameToDaoAdapter(dataFrame: DataFrame): Unit = {

    // first we need RDD from dataframe
    dataFrame.foreachPartition(x => persistRecords(x));

  }

  def transformToList(row: Row): List[Object] = {

    val time1: Long = System.currentTimeMillis();
    val walker: Iterator[Any] = row.toSeq.toIterator
    val rowList = new ListBuffer[Object];
    while (walker.hasNext) {

      var item: Object = walker.next().asInstanceOf[Object];
      rowList += item

    }
    val time2: Long = System.currentTimeMillis();

    return rowList.toList;
  }

  def persistRecords(rowWalker: Iterator[Row]): Unit = {

    logger.info("reached inside persist record ")
    val basicDAO = new CassandraDAO(hostList, schmea, table);
    logger.info("reached inside persist record after creating DAO")
    val rowListWalker: Iterator[List[Object]] = rowWalker.map(x => transformToList(x));

    val time1: Long = System.currentTimeMillis();
    basicDAO.write(rowListWalker.toList);
    val time2: Long = System.currentTimeMillis();
    logger.info("Time spend is persisting   elements is "
      + (time2 - time1));


  }

  def persistRecordsWithTTL(rowWalker: Iterator[Row], ttl: Int): Unit = {

    logger.info("reached inside persist record ")

    val map: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()
    map.put("ttl", ttl.asInstanceOf[Integer])
    val cassandraConfig: CassandraConfig = new CassandraConfig(map);

    val basicDAO = new CassandraDAO(hostList, schmea, table, cassandraConfig);
    logger.info("reached inside persist record after creating DAO")
    val rowListWalker: Iterator[List[Object]] = rowWalker.map(x => transformToList(x));

    val time1: Long = System.currentTimeMillis();


    basicDAO.write(rowListWalker.toList);
    val time2: Long = System.currentTimeMillis();
    logger.info("Time spend is persisting   elements is "
      + (time2 - time1));


  }

  /**
    * Convert RDD to dataframe with validate schema
    * @param testRDD input snmp data
    * @return DataFrame (snmp_raw)
    */

  def processRDD(testRDD: org.apache.spark.rdd.RDD[(String)],
                 sqlContext: SQLContext): DataFrame = {


    val df = if (!testRDD.isEmpty()) {


      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val row = testRDD.map(x => x.toString().split(",")).map {
        x => Row(ip2Long(x(0)), new java.sql.Date(format.parse(x(1).substring(0, 10)).getTime()),
          x(0), x(2), x(3), x(4),
          x(5));
      }
      // now do distinc of all the records

      sqlContext.createDataFrame(row, daoTestSchema)


    } else null
    df.show()
    df
  }

  /**
    * Convert RDD to dataframe with validate schema
    * @param testRDD input snmp data
    * @return DataFrame (snmp_raw)
    */

  def processRDD2(testRDD: org.apache.spark.rdd.RDD[(String)],
                  sqlContext: SQLContext): DataFrame = {


    val df = if (!testRDD.isEmpty()) {


      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val row = testRDD.map(x => x.toString().split(",")).map {
        x => Row(ip2Long(x(0)),
          x(0), x(2), x(3), x(4), new java.sql.Date(format.parse(x(1).substring(0, 10)).getTime()),
          x(5));
      }
      // now do distinc of all the records

      sqlContext.createDataFrame(row, daoTestSchema2)


    } else null
    df.show()
    df
  }

  def ip2Long(ipAddress: String): Long = {
    val ipAddressInArray: Array[String] = ipAddress.split("\\.");
    var result: Double = 0

    var i: Int = 0
    while (i < ipAddressInArray.length) {
      val power: Int = 3 - i
      val ip: Int = ipAddressInArray(i).toInt
      result = result + ip * Math.pow(256, power)
      i = i + 1;
    }


    result.toLong;

  }

  def ipToLong(ipAddress: String): Long = {

    val ipAddressInArray: Array[String] = ipAddress.split("\\.");
    var result: Long = 0;

    for (i <- 0 to ipAddressInArray.length - 1) {

      val power: Integer = 3 - i;
      val ip: Integer = ipAddressInArray(i).toInt;
      result += (ip * Math.pow(256.toDouble, power.toDouble)).toLong;
    }

    return result;
  }


}
