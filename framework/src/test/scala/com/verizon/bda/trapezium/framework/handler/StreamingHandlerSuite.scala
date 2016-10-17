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
package com.verizon.bda.trapezium.framework.handler

import com.verizon.bda.trapezium.framework.hdfs.HdfsDStreamTestSuiteBase

import scala.io.Source

/**
 * @author Pankaj on 9/20/15.
 *         modified by sumanth.venkatasubbaiah
 */
class StreamingHandlerSuite extends HdfsDStreamTestSuiteBase {
  val path1 = "src/test/data/hdfs/hdfs_stream_1.csv"
  val path2 = "src/test/data/hdfs/hdfs_stream_2.csv"

  val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
  val input2: Seq[String] = Source.fromFile(path2).mkString("").split("\n").toSeq

  test("Streaming Handler Test") {
    val inputStream = Seq(input1, input2)
    setupWorkflow("streamWorkFlow", inputStream)
  }

}
