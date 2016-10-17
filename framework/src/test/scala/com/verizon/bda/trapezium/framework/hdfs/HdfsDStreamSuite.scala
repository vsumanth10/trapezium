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
package com.verizon.bda.trapezium.framework.hdfs

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}
import org.scalatest.{Suite, BeforeAndAfterAll, FunSuite}

/**
 * @author Pankaj on 11/19/15.
 */
class HdfsDStreamSuite extends FunSuite with BeforeAndAfterAll { self: Suite =>
  @transient var ssc: StreamingContext = _
  @transient var sparkConf: SparkConf = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig("streamWorkFlow")

    sparkConf = ApplicationManager.getSparkConf(appConfig)
    val checkPointDirectory = workflowConfig.hdfsStream.asInstanceOf[Config]
      .getString("checkpointDirectory")
    ssc = HdfsDStream.createStreamingContext(0, checkPointDirectory, sparkConf)
  }

  override def afterAll() {
    if (ssc != null) {
      ssc.stop(true, true)
    }
    super.afterAll()
  }

  test("HDFS Create StreamingContext Test") {
    assert(ssc.getState() == StreamingContextState.INITIALIZED)
  }

  test("Create DStream Test") {
    val dStreams = HdfsDStream.createDStreams(ssc)
    assert(dStreams("hdfsStream") != null)
  }
}
