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
package org.apache.spark.zookeeper

import java.net.InetSocketAddress
import org.apache.spark.util.Utils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import scala.util.Random

/**
 * @author Pankaj on 11/30/15.
 *         Embedded zookeeper to run local workflow synchronization tests
 */
class EmbeddedZookeeper(val zkConnect: String) {
  val random = new Random()
  val snapshotDir = Utils.createTempDir()
  val logDir = Utils.createTempDir()

  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
  val (ip, port) = {
    val splits = zkConnect.split(":")
    (splits(0), splits(1).toInt)
  }
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress(ip, port), 16)
  factory.startup(zookeeper)

  val actualPort = factory.getLocalPort

  def shutdown() {
    factory.shutdown()
    Utils.deleteRecursively(snapshotDir)
    Utils.deleteRecursively(logDir)
  }
}

