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

import com.verizon.bda.trapezium.framework.kafka.KafkaSink
import com.verizon.bda.trapezium.framework.{ApplicationManager, StreamingTransaction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
  * @author sumanth.venkatasubbaiah
  *         KafkaSink test transactions
  */

/**
  * TestKafkaSinkTxn1 writes a DStream into a Kafka topic
  */
object TestKafkaSinkTxn1 extends StreamingTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)
  var batchID = 0

  override def preprocess(sc: SparkContext): Unit = {
    logger.info("Inside preprocess of TestKafkaSinkTxn1")
  }

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    logger.info("Inside TestKafkaSinkTxn1.processStream")
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {

    val kafkaConf = ApplicationManager.getKafkaConf()
    val kafkaSink = rdd.sparkContext.broadcast(KafkaSink(kafkaConf))
    val kafkaTopic = "topic2"

    logger.info("Inside TestKafkaSinkTxn1.persistStream")
    logger.info(s"Sending messages to kafka topic - ${kafkaTopic}")

    rdd.foreach(row => {
      val msg = row.toString.replaceAll("\\[", "").replaceAll("\\]", "")
      kafkaSink.value.send(kafkaTopic, msg)
    })
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {
  }
}

/**
  * TestKafkaSinkTxn2 reads from the Kafka topic previously written
  */
object TestKafkaSinkTxn2 extends StreamingTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)
  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    logger.info("Inside TestKafkaSinkTxn2.processStream")
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    logger.info("Inside TestKafkaSinkTxn2.persistStream")
    require(rdd.count == 512)
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}
