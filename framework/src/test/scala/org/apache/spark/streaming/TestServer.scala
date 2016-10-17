/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{ServerSocket, Socket, SocketException}
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}

import org.apache.spark.Logging

import scala.language.postfixOps


/** This is a server to test the network input stream */
class TestServer(portToBind: Int = 0) extends Logging {

  val queue = new ArrayBlockingQueue[String](100)

  val serverSocket = new ServerSocket(portToBind)

  private val startLatch = new CountDownLatch(1)

  val servingThread = new Thread() {
    override def run() {
      try {
        while (true) {
          logInfo("Accepting connections on port " + port)
          val clientSocket = serverSocket.accept()
          if (startLatch.getCount == 1) {
            // The first connection is a test connection to implement "waitForStart", so skip it
            // and send a signal
            if (!clientSocket.isClosed) {
              clientSocket.close()
            }
            startLatch.countDown()
          } else {
            // Real connections
            logInfo("New connection")
            try {
              clientSocket.setTcpNoDelay(true)
              val outputStream = new BufferedWriter(
                new OutputStreamWriter(clientSocket.getOutputStream))

              while (clientSocket.isConnected) {
                val msg = queue.poll(100, TimeUnit.MILLISECONDS)
                if (msg != null) {
                  outputStream.write(msg)
                  outputStream.flush()
                  logInfo("Message '" + msg + "' sent")
                }
              }
            } catch {
              case e: SocketException => logError("TestServer error", e)
            } finally {
              logInfo("Connection closed")
              if (!clientSocket.isClosed) {
                clientSocket.close()
              }
            }
          }
        }
      } catch {
        case ie: InterruptedException =>

      } finally {
        serverSocket.close()
      }
    }
  }

  def start(): Unit = {
    servingThread.start()
    if (!waitForStart(10000)) {
      stop()
      throw new AssertionError("Timeout: TestServer cannot start in 10 seconds")
    }
  }

  /**
   * Wait until the server starts. Return true if the server starts in "millis" milliseconds.
   * Otherwise, return false to indicate it's timeout.
   */
  private def waitForStart(millis: Long): Boolean = {
    // We will create a test connection to the server so that we can make sure it has started.
    val socket = new Socket("localhost", port)
    try {
      startLatch.await(millis, TimeUnit.MILLISECONDS)
    } finally {
      if (!socket.isClosed) {
        socket.close()
      }
    }
  }

  def send(msg: String) { queue.put(msg) }

  def stop() { servingThread.interrupt() }

  def port: Int = serverSocket.getLocalPort
}
