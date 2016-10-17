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
package com.verizon.bda.trapezium.validation

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

class ValidateColumnSuite extends FunSuite  with LocalSparkContext {



  val path = "src/test/data/hdfs/source1"
  val validatorConfig = ValidationPreparer.getValidationConfig()
  var parser : CSVParser = null
  def setParser(delimter : Char) : Unit = {
    parser = new CSVParser(delimter)
  }
  val goodRow = "1445459784,8262103,3e12260269c420b7,\"173.192.82.194\"," +
    "1,\"realtime.services.disqus.com\",\"/ws/2/thread/4243265377\"," +
    "\"Mozilla/5.0 (iPhone; CPU iPhone OS 9_0_2 like Mac OS X) " +
    "AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13A452 " +
    "[FBAN/FBIOS;FBAV/41.0.0.22.268;FBBV/15798540;FBDV/iPhone5,2;FBMD/iPhone;FB\"," +
    "\"text/html\",0,0,583,3500,\"\",4,\"\",\"HI0\",\"\",0,1"

  val badRow = "badrecords3e12260269c420b7,\"173.192.82.194\",1," +
    "\"realtime.services.disqus.com\",\"/ws/2/thread/4243265377\"," +
    "\"Mozilla/5.0 (iPhone; CPU iPhone OS 9_0_2 like Mac OS X) " +
    "AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13A452 " +
    "[FBAN/FBIOS;FBAV/41.0.0.22.268;FBBV/15798540;FBDV/iPhone5,2;" +
    "FBMD/iPhone;FB\",\"text/html\",0,0,583,3500,\"\",4,\"\",\"HI0\",\"\",0,1"

  val numberOfCol = validatorConfig.getList(Constants.Column).size

  test("Validation module: test validateNumberColumn  ") {
    setParser(',')
    val goodArray = parser.parseLine(goodRow)
    val badArray = parser.parseLine(badRow)
    assert(ValidateColumn.validateNumberOfColumns(goodArray, numberOfCol))
    assert(!ValidateColumn.validateNumberOfColumns(badArray, numberOfCol))
  }



}






