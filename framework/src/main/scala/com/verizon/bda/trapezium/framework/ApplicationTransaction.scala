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
package com.verizon.bda.trapezium.framework

import java.sql.Time

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.dstream.DStream

/**
 * @author Pankaj on 9/1/15.
 *         debasish83 updated based on 11/13/15 discussion to support DAL
 *         and model training on batch data
 */
trait ApplicationTransaction extends Serializable {
  def preprocess(sc: SparkContext): Unit = {}
}

trait StreamingTransaction extends ApplicationTransaction {

  def processStream(dStreams: Map[String, DStream[Row]],
                    workflowTime: Time): DStream[Row]

  def persistStream(rdd: RDD[Row], batchTime: Time): Unit = {}

  def rollbackStream(batchTime: Time): Unit = {}
}

trait BatchTransaction extends ApplicationTransaction {

  def processBatch(df: Map[String, DataFrame],
              workflowTime: Time): DataFrame

  def persistBatch(df: DataFrame, batchTime: Time): Unit = {}

  def rollbackBatch(batchTime: Time): Unit = {}
}

/**
 * ModelTransaction behavior
 * train model on batch using train(rdd): Use DAL to store the model
 * score model on batch using process(rdd)
 * score model on stream using process(dstream) through extending
 * StreamingApplicationTransaction
 */
trait ModelTransaction extends BatchTransaction {
  def train(df: Map[String, DataFrame], workflowTime: Time): Boolean
}
