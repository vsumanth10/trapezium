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

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
import org.scalatest.{BeforeAndAfter, FunSuite}
// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on
/**
  * @author hutashan
  *         tests for source generators
  */
object FileCopy {

  def copyFiles (numberOffiles : Int, conditions : String = null): Unit = {
    // Create 10 new files
    ( 1 to numberOffiles ).foreach { i => {
      var cal = Calendar.getInstance()
      cal.add(Calendar.DATE, - i)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        createFiles(dateFormat.format(cal.getTime()) , i, conditions)
        cal.add(Calendar.HOUR, -i)
        createFiles(dateFormat.format(cal.getTime()), i, conditions)
      }
    }
  }
  def createFiles (fileDate : String, fileCounter : Int, conditions : String = null) : Unit = {
    val sourceMIDM = "src/test/data/hdfs/midmsmall/file1.csv"
    val destinationMIDM = {
      if (conditions == null) {
        "src/test/data/hdfs/filesplit/new_file" + fileCounter + fileDate + "/midm.txt"
      } else {
        if (fileCounter%2 == 0) {
          "src/test/data/hdfs/filesplit/new_file" +
             fileDate + fileCounter +conditions + "/midm.txt"
        } else {
          "src/test/data/hdfs/filesplit/new_file" + fileCounter +fileDate + "/midm.txt"
        }
      }
    }
    val scrPath = new Path(sourceMIDM)
    val destinationPath = new Path(destinationMIDM)
    val conf = new Configuration
    val uri = new URI(sourceMIDM)
    val fsystem = FileSystem.get(uri, conf)
    fsystem.copyFromLocalFile(scrPath, destinationPath)

  }





  def fileDelete(): Unit = {
    val fs = new RawLocalFileSystem
    val destinationMIDM = "src/test/data/hdfs/filesplit"
    val scrPath = new Path(destinationMIDM)
    val conf = new Configuration
    val uri = new URI(destinationMIDM)
    val fsystem = FileSystem.get(uri, conf)
    fsystem.delete(scrPath, true)
  }

  def fileDelete(path : String): Unit = {
    val fs = new RawLocalFileSystem
    val scrPath = new Path(path)
    val conf = new Configuration
    val uri = new URI(path)
    val fsystem = FileSystem.get(uri, conf)
    fsystem.delete(scrPath, true)
  }
}
