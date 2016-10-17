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

import java.util.concurrent.locks.ReentrantLock

/**
 * @author debasish83 on 2/28/16.
 *         Synchronize thread running TimerTask and thread running BatchHandler.scheduleBatchRun
 *         without busy wait using Thread.sleep
 */
private[framework] trait Waiter {
  // TO DO : Re-use ContextWaiter from Spark streaming
  private val relock = new ReentrantLock()
  private val condition = relock.newCondition()

  // Guarded by "lock"
  private var stopHandler: Boolean = false

  // Guarded by "lock"
  private var error: Throwable = null

  def notifyError(e: Throwable): Unit = {
    relock.lock()
    try {
      error = e
      condition.signalAll()
    } finally {
      relock.unlock()
    }
  }

  def notifyStop(): Unit = {
    relock.lock()
    try {
      stopHandler = true
      condition.signalAll()
    } finally {
      relock.unlock()
    }
  }

  /**
   * Return `true` if it's stopped; or throw the reported error if `notifyError` has been called; or
   * `false` if the waiting time detectably elapsed before return from the method.
   */
  def waitForStopOrError: Boolean = {
    relock.lock()
    try {
      while (!stopHandler && error == null) condition.await()
      // If already had error, then throw it
      if (error != null) throw error
      // already stopped
      stopHandler
    } finally {
      relock.unlock()
    }
  }
}
