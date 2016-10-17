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

import java.io.File
import java.util
import java.util.Properties

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationListener
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import kafka.admin.AdminUtils
import kafka.common.{TopicAndPartition, KafkaException}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.io
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{MutableList => MList}
import scala.collection.mutable.{Map => MMap}

/**
 * @author Pankaj on 3/8/16.
 */
class KafkaTestSuiteBase extends FunSuite with BeforeAndAfter {

  val logger = LoggerFactory.getLogger(this.getClass)

  private var zkList: String = _
  private var zk: EmbeddedZookeeper = _
  private val zkConnectionTimeout = 6000
  private val zkSessionTimeout = 6000
  private var kafkaBrokers: String = _
  private var brokerConf: KafkaConfig = _
  private var server: KafkaServer = _
  private var server2: KafkaServer = _
  private var brokerPort2 = 9093
  private var brokerConf2: KafkaConfig = _
  private var producer: Producer[String, String] = _
  private var zkReady = false
  private var brokerReady = false

  protected var zkClient: ZkClient = _

  before {

    // Load the config file
    val appConfig = ApplicationManager.getConfig()
    kafkaBrokers = appConfig.kafkabrokerList
    zkList = appConfig.zookeeperList

    logger.info("Kafka broker list " + kafkaBrokers)

    // set up local Kafka cluster
    setupKafka
  }

  after {
    tearDownKafka
  }

  private def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkList"
  }

  private def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$kafkaBrokers"
  }

  private def setupKafka() {

    // Zookeeper server startup
    zk = new EmbeddedZookeeper(s"$zkList")
    // Get the actual zookeeper binding port
    zkReady = true
    logger.info("==================== Zookeeper Started ====================")

    zkClient = new ZkClient(zkAddress, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    logger.info("==================== Zookeeper Client Created ====================")

    // Kafka broker startup
    var bindSuccess: Boolean = false
    while (!bindSuccess) {
      try {
        val brokerProps = getBrokerConfig()
        brokerConf = new KafkaConfig(brokerProps)
        server = new KafkaServer(brokerConf)
        server.startup()
        logger.info("==================== Kafka Broker Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          throw new Exception("Socket server failed to bind to", e)
        /*
        if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
          brokerPort += 1
        }
        */
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }

    Thread.sleep(1000)
    logger.info("==================== Kafka + Zookeeper Ready ====================")
    brokerReady = true
  }

  // Method added to enable testing of the broker failure recovery
  private def justShutdownBroker(): Unit = {
    server.shutdown()
  }

  private def startSecondBroker(): Unit = {
    var bindSuccess: Boolean = false
    while (!bindSuccess) {
      try {
        val brokerProps = getBrokerConfig()
        brokerProps.put("broker.id", "1")
        brokerProps.put("port", brokerPort2.toString)
        brokerProps.put("log.dir", "/tmp/kafka")
        // brokerProps.put("log.dir", KafkaTestUtils.createTempDir().getAbsolutePath)
        brokerConf2 = new KafkaConfig(brokerProps)
        server2 = new KafkaServer(brokerConf2)
        server2.startup()
        logger.info("==================== Second Kafka Broker Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
            brokerPort2 += 1
          }
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }
  }

  // Method added to enable testing of the broker failure recovery
  private def restartBroker(): Unit = {
    var bindSuccess: Boolean = false
    while (!bindSuccess) {
      try {
        /*
        val brokerProps = getBrokerConfig()
        brokerConf = new KafkaConfig(brokerProps)
        server = new KafkaServer(brokerConf)
        */
        server.startup()
        logger.info("==================== Kafka Broker Re-Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          throw new Exception("Socket server failed to bind to", e)
        /*
        if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
          brokerPort += 1
        }
        */
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }

    Thread.sleep(1000)
    logger.info("==================== Kafka + Zookeeper Ready Again ====================")
    brokerReady = true
  }

  private def tearDownKafka() {
    brokerReady = false
    zkReady = false
    if (producer != null) {
      producer.close()
      producer = null
    }

    if (server != null) {
      server.shutdown()
      server = null
    }
    if (server2 != null) {
      server2.shutdown()
      server2 = null
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    // Close ZooKeeper connections
    ZooKeeperConnection.close

    // shutdown zookeeper
    if (zk != null) {
      zk.shutdown()
      zk = null
    }

  }

  def createTopic(topic: String, nparts: Int = 1) {
    if (!AdminUtils.topicExists(zkClient, topic)) {

      AdminUtils.createTopic(zkClient, topic, nparts, 1)
      // wait until metadata is propagated
      // waitUntilMetadataIsPropagated(topic, 0)
      logger.info(s"==================== Topic $topic Created ====================")

    } else {

      logger.info(s"================= Topic $topic already exists ================")
    }
  }

  private def sendMessages(topic: String, messageToFreq: Map[String, Int]) {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }

  private def sendMessages(topic: String, messages: Array[String]) {
    producer = new Producer[String, String](new ProducerConfig(getProducerConfig()))
    // producer.send(messages.map { new KeyedMessage[String, String](topic, _ ) }: _*)
    producer.send(messages.map {
      new KeyedMessage[String, String](topic, null, _)
    }: _*)
    producer.close()
    logger.info(s"=============== Sent Messages ===================")
  }


  private def deleteRecursively(in : File): Unit = {

    if ( in.isDirectory ) {
      io.FileUtils.deleteDirectory(in)
    } else {
      in.delete()
    }
  }

  private def getBrokerConfig(): Properties = {
    val kafkaBrokerList = kafkaBrokers.split(",")
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", kafkaBrokerList(0).split(":")(0))
    props.put("port", kafkaBrokerList(0).split(":")(1))

    deleteRecursively( new File("/tmp/kafka"))
    props.put("log.dir", "/tmp/kafka")
    // props.put("log.dir", KafkaTestUtils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  private def getProducerConfig(): Properties = {
    var brokerAddr = brokerConf.hostName + ":" + brokerConf.port
    if (brokerConf2 != null) brokerAddr += "," + brokerConf2.hostName + ":" + brokerConf2.port
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }

  /*
   private def waitUntilMetadataIsPropagated(topic: String, partition: Int) {
     eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
       assert(
         server.apis.metadataCache.containsTopicAndPartition(topic, partition),
         s"Partition [$topic, $partition] metadata not propagated after timeout"
       )
     }
   }
   */

  def setupWorkflow(workflowName: String, inputSeq: Seq[Seq[String]]): Unit = {

    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")

    val topicName = streamsInfo.get(0).getString("topicName")
    val newInputSeq = inputSeq.map(seq => Seq((topicName, seq)))

    setupWorkflowForMultipleTopics(workflowName, newInputSeq)

  }

  def setupWorkflowForMultipleTopics(workflowName: String,
                                     inputSeq: Seq[Seq[(String, Seq[String])]]): Unit = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")

    val topics: MList[String] = MList()
    streamsInfo.asScala.foreach(streamInfo => {

      val topicName = streamInfo.getString("topicName")
      logger.debug("Kafka stream topic - " + topicName)

      topics += topicName
      createTopic(topicName)

    })

    val currentTimeStamp = System.currentTimeMillis()
    ApplicationManager.updateWorkflowTime(currentTimeStamp)

    val ssc = startKafkaWorkflow(topics)

    // start streaming
    ssc.start

    inputSeq.foreach( input => {

      input.foreach( seq => {

        logger.info(s"Size of the input: ${seq._2.size}")
        sendMessages(seq._1, seq._2.toArray)

      })

      Thread.sleep(kafkaConfig.getLong("batchTime") * 1000)

    })

    ssc.awaitTerminationOrTimeout(
      kafkaConfig.getLong("batchTime") * 1000)

    if( ssc != null ) {
      logger.info(s"Stopping streaming context from test Thread.")
      ssc.stop(true, false)

      // reset option
      KafkaDStream.sparkcontext = None
    }

    assert (!ApplicationManager.stopStreaming)

  }


  def startKafkaWorkflow(topics: MList[String]): StreamingContext = {

    // Load the config file
    val applicationConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.getWorkflowConfig()

    val runMode = workflowConfig.runMode

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val sparkConf = ApplicationManager.getSparkConf(applicationConfig)

    val ssc = KafkaDStream.createStreamingContext(sparkConf)

    val topicPartitionOffsets = MMap[TopicAndPartition, Long]()

    topics.foreach( topicName => {

      val partitionOffset =
        KafkaDStream.fetchPartitionOffsets(topicName, runMode, applicationConfig)
      topicPartitionOffsets ++= partitionOffset
    })

    val dStreams = KafkaDStream.createDStreams(ssc, applicationConfig.kafkabrokerList,
      kafkaConfig, topicPartitionOffsets.toMap, applicationConfig)

    ApplicationManager.runStreamWorkFlow(dStreams)

    // add streaming listener
    val listener: ApplicationListener = new ApplicationListener(workflowConfig)
    ssc.addStreamingListener(listener)
    ssc
  }
}
