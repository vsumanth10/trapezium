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
package com.verizon.bda.trapezium.framework.manager

import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.kafka.KafkaDStream
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.validation.DataValidator
import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory;

/**
 * @author Pankaj on 9/2/15.
 */
private[framework]
class ApplicationListener(workflowConfig: WorkflowConfig) extends StreamingListener {

  val logger = LoggerFactory.getLogger(this.getClass)
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    logger.info(s"Inside onBatchCompleted:" +
      s" ${batchCompleted.batchInfo.numRecords}" +
      s" ${batchCompleted.batchInfo.batchTime}")

    ApplicationUtils.updateCurrentWorkflowTime(
      workflowConfig.workflow,
      batchCompleted.batchInfo.batchTime.milliseconds,
      ApplicationManager.getConfig().zookeeperList)

    // clear accumulators
    logger.info(s"We need to reset accumulators")
    DataValidator.resetAccumulators

    workflowConfig.dataSource match {

      case "KAFKA" => {

        if (KafkaDStream.saveKafkaStreamOffsets (workflowConfig)) {
          ApplicationManager.synchronized {
            ApplicationManager.notifyAll()
          }
          logger.info("Detected new partition, notify restart streaming context")

        }
      }

    }

  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {

    logger.info(s"Inside onBatchStarted ${batchStarted.batchInfo.batchTime}")
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {

    logger.info(s"Inside onBatchSubmitted ${batchSubmitted.batchInfo.batchTime}")
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {

    logger.info("Inside onReceiverError")
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {

    logger.info("Inside onReceiverStarted")
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {

    logger.info("Inside onReceiverStopped")
  }
}
