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
package com.verizon.bda.trapezium.framework.kafka

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.slf4j.LoggerFactory

/**
  * @author sumanth.venkatasubbaiah
  * @author roy.debjyoti
  * Class to create a Kafka producer instance and post messages to a Kafka topic
  */

class KafkaSink(createProducer: () => KafkaProducer[String, String])
  extends Serializable {
  lazy val producer = createProducer()
  def send(topic: String, value: String): Unit = {
    val callBack = new KafkaSinkExceptionHandler
    producer.send(new ProducerRecord(topic, value), callBack)
    callBack.throwExceptionIfAny()
  }
}

object KafkaSink {
  val logger = LoggerFactory.getLogger(this.getClass)

  def apply(config: Map[String, Object]): KafkaSink = {
    val f = () => {
      val prop = new Properties()
      config.keysIterator.foreach(k => prop.put(k, config.get(k).get))
      val producer = new KafkaProducer[String, String](prop)
      sys.addShutdownHook {
        logger.info("Closing the Kafka producer instance")
        producer.close()
      }
      logger.info(s"Creating a Kafka Producer instance with properties -> ${prop}")
      producer
    }
    new KafkaSink(f)
  }
}

/**
  * Exception handler for Kafka posts from Spark
  */
class KafkaSinkExceptionHandler extends Callback {

  import java.util.concurrent.atomic.AtomicReference
  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    lastException.set(Option(exception))
  }

  def throwExceptionIfAny(): Unit = {
    lastException.getAndSet(None).foreach(ex => throw ex)
  }
}
