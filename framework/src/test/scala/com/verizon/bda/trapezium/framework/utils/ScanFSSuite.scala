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
package com.verizon.bda.trapezium.framework.utils

import java.io.{BufferedWriter, FileWriter, File}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.slf4j.LoggerFactory

/**
  * @author sumanth.venkatasubbaiah
  *         Tests ScanHDFS functionality
  */
class ScanFSSuite extends FunSuite with BeforeAndAfterAll {

  val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val conf = new Configuration
  @transient lazy val testDir = "target/scanDir"
  @transient lazy val MTIME = 3

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create a temp directory
    val tempDir = s"${testDir}/temp"
    val fs = new RawLocalFileSystem
    fs.setConf(conf)
    fs.mkdirs(new Path(tempDir))
    // Create 10 new files
   ( 1 to 10 ).foreach { i => {
      val destFile = s"$tempDir/${i}.txt"

      fs.createNewFile(new Path(destFile))

      val br = new BufferedWriter(new FileWriter(destFile))
      br.write(s"this is ${destFile}\n")
      br.close
    }
    }

    // Change the modify timestamp of 5 of these new files
    ( 1 to 10 ).filter{_%2==0}.foreach{ i => {
      val destFile = s"$tempDir/${i}.txt"
      fs.setTimes(new Path(destFile), MTIME, 1)
    }}

    var tempChildDire = s"${testDir}"
    // Create 10 new files
    ( 1 to 10 ).foreach { i => {
      val destFile = tempChildDire + "/file" + i
      tempChildDire = tempChildDire + "/" + i
      fs.createNewFile(new Path(destFile))

      val br = new BufferedWriter(new FileWriter(destFile))
      br.write(s"this is ${destFile}\n")
      br.close
    }
    }
    tempChildDire = s"${testDir}"
    // Change the modify timestamp of 5 of these new files
    ( 1 to 10 ).foreach{ i => {
      val destFile = tempChildDire + "/file" + i
      if (i%2 == 0) fs.setTimes(new Path(destFile), MTIME, 1)
      tempChildDire = tempChildDire + "/" + i
    }}





  }

  test("get new files ") {
    val newFiles: Array[String] = ScanFS.getFiles(testDir, 15)
   assert(10 == newFiles.size)
   val files = newFiles.mkString(",")
    logger.info(s"Files available for this batch : ${files} ")
    assert(files.contains("1.txt"))
    assert(files.contains("3.txt"))
    assert(files.contains("5.txt"))
    assert(files.contains("7.txt"))
    assert(files.contains("9.txt"))
    assert(!files.contains("4.txt"))
    assert(files.contains("file1"))
    assert(files.contains("file3"))
    assert(files.contains("file5"))
    assert(files.contains("file7"))
    assert(files.contains("file9"))
    assert(!files.contains("file4"))


  }
  test("get new files from dir") {
    val newFiles: Array[String] = ScanFS.getFiles(testDir, -1L)
    assert(20 == newFiles.size)
    val files = newFiles.mkString(",")
    logger.info(s"Files available for this batch : ${files} ")
    assert(files.contains("1.txt"))
    assert(files.contains("3.txt"))
    assert(files.contains("5.txt"))
    assert(files.contains("7.txt"))
    assert(files.contains("9.txt"))
    assert(files.contains("4.txt"))
    assert(files.contains("file1"))
    assert(files.contains("file3"))
    assert(files.contains("file5"))
    assert(files.contains("file7"))
    assert(files.contains("file9"))
    assert(files.contains("file4"))
  }

  test("file special") {
    val testDir = "target/scanDir/*/*/*e*"
    val newFiles: Array[String] = ScanFS.getFiles(testDir, -1L)
    val files = newFiles.mkString(",")
    logger.info(s"Files available for this batch : ${files} ")
    assert(newFiles.size==1)
  }

  override def afterAll(): Unit = {
    // Delete the temp directory
    val fs = new RawLocalFileSystem
    fs.delete(new Path(testDir), true)
    super.afterAll()

  }
}
