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
package com.verizon.bda.trapezium.framework.zookeeper

import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.ZooKeeper.States
import org.slf4j.LoggerFactory

/**
 * @author Pankaj on 10/21/15.
 */
private[framework] object ZooKeeperConnection {

  val logger = LoggerFactory.getLogger(this.getClass)
  var zkc: ZooKeeper = _

  def create(zookeeperList: String): ZooKeeper = {

    if (zkc == null || !zkc.getState.isConnected) {

      synchronized {

        // check if zkc is still null
        if (zkc == null || !zkc.getState.isConnected) {
          logger.info(s"Creating ZooKeeper connection ${zookeeperList}")
          val zkcTemp = new ZooKeeper(zookeeperList, 30 * 60 * 1000, ZooKeeperWatcher)

          // for local testing
          if (zookeeperList.contains("localhost")) {
            try {
              var retryAttempts = 0
              while(!zkcTemp.getState.equals(States.CONNECTED)
                && retryAttempts < 10) {

                logger.info(s"ZooKeeper connection state is ${zkcTemp.getState.name}")
                Thread.sleep(1000)
                retryAttempts+=1
              }
            } catch {
              case ex: Throwable => {
                ex.printStackTrace()
              }
            }
          }

          // This reassignment is needed to assign ZooKeeper connection after the sleep
          // Needed only for unit tests
          zkc = zkcTemp
        }
      }
    } else {

      logger.info(s"Reusing ZooKeeper connection and state is ${zkc.getState.name}")
    }
    zkc
  }

  def close: Unit = {

    if (zkc != null && zkc.getState.equals(States.CONNECTED)){

      zkc.close()
    }

  }
}
