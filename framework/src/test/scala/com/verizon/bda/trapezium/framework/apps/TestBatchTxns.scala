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

import java.sql.{Date, Time}

import com.verizon.bda.trapezium.framework.BatchTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.slf4j.LoggerFactory


/**
 * @author sumanth.venkatasubbaiah
 *         Various Test Batch transactions
 *
 */

object TestBatchTxn1 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  private val CONST_STRING = "This has to be populated in the preprocess method"
  var populateFromPreprocess: String = _

  override def preprocess(sc: SparkContext): Unit = {
    logger.info("Inside preprocess of TestBatchTxn1")
    populateFromPreprocess = CONST_STRING
  }

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn1")
    require(df.size > 0)
    require(populateFromPreprocess == CONST_STRING)
    val inData = df("source1")
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
    logger.info(s"Count ${df.count}")
    require(populateFromPreprocess == CONST_STRING)
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn1

object TestBatchTxn2 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn2")
    require(df.size > 0)
    val inData = df("source2")
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    logger.info(s"Count ${df.count}")
    require(df.count == 499 )
  }

  override def rollbackBatch(batchTime: Time): Unit = {

  }
} // end TestBatchTxn2


object TestBatchTxn3 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn3")
    require(df.size > 0)
    val inData1 = df("txn1Output")
    require(inData1.count > 0 )
    val inData2 = df("txn2Output")
    require(inData2.count > 0)
    inData1
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn3

object TestBatchTxn4 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn4")
    require(df.size > 0)
    val inData = df("onlyDirTrue")
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}
object TestFileSplit extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time)
  : DataFrame = {

    logger.info("Inside process of TestFileSplit")
    require(df.size > 0)

    val inData1 = df("testDataSplitFiles")
    logger.info("count for this run is : " + inData1.count() + " worflow time is "
      + new Date (wfTime.getTime).toString)


    (inData1)
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn5 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn4")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn5

object TestBatchTxn6 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn6")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
    df.write.parquet("/target/testdata/TestBatchTxn6")
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn7 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn7")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
    df.write.parquet("target/testdata/TestBatchTxn7")
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn8 extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn8")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
    require(df.count > 0)
    df.write.parquet("target/testdata/TestBatchTxn8")
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}


