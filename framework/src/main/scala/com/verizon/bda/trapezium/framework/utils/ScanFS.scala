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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
/**
  * @author sumanth.venkatasubbaiah
  *         Adopted from ScanHDFS.java from ETLJobFramework project
  */
object ScanFS {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param inDir the input hdfs dir to watch for new files
    * @param timeStamp time in milliseconds to get the new files
    * @return array of filenames that are to be considered for this batch processing
    */
  def getFiles(inDir: String, timeStamp: Long ): Array[String] = {
    logger.info(s"Getting list of files from dir " +
      s"$inDir that are newer than timestamp $timeStamp")

    val uri: URI = new URI(inDir)
    val config: Configuration = new Configuration
    val fileSystem: FileSystem = FileSystem.get(uri, config)
    val hdfsPath: Path = new Path(inDir)
    val fileStatus: Array[FileStatus] = fileSystem.globStatus(hdfsPath)

    def scanfile(fileStatus: Array[FileStatus],
                 fileArray: ArrayBuffer[String] = ArrayBuffer[String]()): ArrayBuffer[String] = {
      try {
        if (fileStatus != null) {
          fileStatus.foreach { file => {
            val fileType: String = file.getPath.getName
            if (fileType != null) {
              if (file.isDirectory) {
                val batchFolders = fileSystem.listStatus(file.getPath)
                scanfile(batchFolders, fileArray)
              } else {
                val folderModificationTime = file.getModificationTime
                if (folderModificationTime > timeStamp && file.getLen > 0) {
                  fileArray += file.getPath.toString
                }
              }
            }
          }
          }
        }
      } catch {
          case e : Throwable =>
            logger.error(s"Error while scanning file", e)
      }
      fileArray
    }

    scanfile(fileStatus).toArray
  }







}
