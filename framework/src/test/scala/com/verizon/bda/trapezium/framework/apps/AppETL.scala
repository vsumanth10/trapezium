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
package com.verizon.bda.trapezium.framework.apps

import java.sql.Time

import com.verizon.bda.trapezium.framework.StreamingTransaction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
 * @author by Pankaj on 10/28/15.
 */
object AppETL extends StreamingTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  var batchID = 0

  private val CONST_STRING = "This has to be populated in the preprocess method"
  var populateFromPreprocess: String = _

  override def preprocess(sc: SparkContext): Unit = {
    logger.info("Inside preprocess of AppETL")
    populateFromPreprocess = CONST_STRING
  }

  override def processStream(dStreams: Map[String, DStream[Row]],
                       batchtime: Time): DStream[Row] = {
    logger.info("Inside ETL")
    val dStream = dStreams.head._2
    require(populateFromPreprocess == CONST_STRING)
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    if (batchID == 0) require(rdd.count() == 490)
    if (batchID == 1) require(rdd.count() == 499)
    require(populateFromPreprocess == CONST_STRING)
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}
