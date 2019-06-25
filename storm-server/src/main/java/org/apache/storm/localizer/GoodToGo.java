/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.localizer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/**
 * Used as a way to give feedback that the listener is ready for the caller to change the blob. By calling @{link GoodToGo#getLatch()} the
 * listener indicates that it wants to block changing the blob until the CountDownLatch is triggered with a call to @{link
 * CountDownLatch#countDown()}.
 */
public class GoodToGo {
    private final GoodToGoLatch latch;
    private boolean gotLatch = false;

    public GoodToGo(CountDownLatch latch, Future<Void> doneChanging) {
        this.latch = new GoodToGoLatch(latch, doneChanging);
    }

    /**
     * Get the latch and indicate that you want to block the blob being changed.
     *
     * @return the latch to use when you are ready.
     */
    public synchronized GoodToGoLatch getLatch() {
        gotLatch = true;
        return latch;
    }

    synchronized void countDownIfLatchWasNotGotten() {
        if (!gotLatch) {
            latch.countDown();
        }
    }

    public static class GoodToGoLatch {
        private final CountDownLatch latch;
        private final Future<Void> doneChanging;
        private boolean wasCounted = false;

        public GoodToGoLatch(CountDownLatch latch, Future<Void> doneChanging) {
            this.latch = latch;
            this.doneChanging = doneChanging;
        }

        public synchronized Future<Void> countDown() {
            if (!wasCounted) {
                latch.countDown();
                wasCounted = true;
            }
            return doneChanging;
        }
    }
}
