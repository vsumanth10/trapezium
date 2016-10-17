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


import scala.io.Source

/**
  * @author sumanth.venkatasubbaiah
  *         KafkaSink Test suite
  */
class KafkaSinkTestSuite extends KafkaTestSuiteBase {

  val path1 = "src/test/data/json/file1.json"
  val path2 = "src/test/data/json/file2.json"

  val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
  val input2: Seq[String] = Source.fromFile(path2).mkString("").split("\n").toSeq

  test("test kafka sink workflows") {
    createTopic("topic2")

    // Write the messages to a kafka topic via a Streaming transaction
    setupWorkflow("kafkaSinkWF1", Seq(input1, input2))
    Thread.sleep(200)

    // Consume the previously written messages in a different Streaming transaction
    setupWorkflow("kafkaSinkWF2", Seq())
    Thread.sleep(200)
  }
}
