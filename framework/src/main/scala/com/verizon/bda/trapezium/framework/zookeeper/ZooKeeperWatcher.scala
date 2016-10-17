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

import org.apache.zookeeper.{WatchedEvent, Watcher}

/**
 * @author Pankaj on 10/12/15.
 */
private[framework] object ZooKeeperWatcher extends Watcher{
  override def process(watchedEvent: WatchedEvent): Unit = {

  }
}
