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

/**
  * Created by v468328 on 3/7/16.
  */

import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by v468328 on 2/29/16.
  */
class CassandraDAOUtils(val hostList: List[String],
                        val schmea: String, val table: String,
                        implicit val sqlContext: CassandraSQLContext) extends Serializable {


  val daoTestSchema = StructType(
    Seq(StructField("ipaddress", LongType, true),
      StructField("processdate", DateType, true),
      StructField("origipaddress", StringType, true),
      StructField("color", StringType, true),
      StructField("description", StringType, true),
      StructField("reason", StringType, true),
      StructField("notes", StringType, true)));


  def persistWithDataFrameToDaoAdapter(dataFrame: DataFrame): Unit = {

    val basicDAO = new CassandraDAO(schmea, table, hostList);
    basicDAO.write(dataFrame)

  }


  /**
    * Convert RDD to dataframe with validate schema
    * @param ipreputationRDD input snmp data
    * @return DataFrame (snmp_raw)
    */

  def processRDD(ipreputationRDD: org.apache.spark.rdd.RDD[(String)],
                 sqlContext: SQLContext): DataFrame = {


    val df = if (!ipreputationRDD.isEmpty()) {


      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val row = ipreputationRDD.map(x => x.toString().split(",")).map {
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

}
