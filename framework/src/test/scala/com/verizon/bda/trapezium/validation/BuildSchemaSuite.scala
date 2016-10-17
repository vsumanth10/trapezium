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


import org.apache.spark.streaming.TestSuiteBase

class BuildSchemaSuite extends TestSuiteBase {
  val validatorConfig = ValidationPreparer.getValidationConfig()
  val validatorConfig1 = ValidationPreparer.getValidationConfigSources2()

  test("Test BuildSchema") {
    schemaTest
  }

  test("Test matchNumberOfColAndDatatype ") {
    matchNumberOfColAndDatatype
  }

  def matchNumberOfColAndDatatype() {
  assert(SchemaBuilder.matchNumberOfColAndDatatype(validatorConfig))
  }

  def schemaTest(): Unit = {
   val schema = SchemaBuilder.buildSchema(validatorConfig)
    val column = validatorConfig.getStringList(Constants.Column)
    val datatype = validatorConfig.getStringList(Constants.DataType)
    assert(schema.nonEmpty)
    assert(column.size() == schema.length)
    assert(datatype.size() == schema.length)
    val fieldName = schema.fieldNames
    for (i <- 0 until fieldName.length) {
      assert(column.get(i) == fieldName(i))
    }
    val badvalidatorConfig = ValidationPreparer.getBadValidationConfig

    intercept[Exception] {
      val schemaNegative = SchemaBuilder.buildSchema(badvalidatorConfig)
    }
  }
}
