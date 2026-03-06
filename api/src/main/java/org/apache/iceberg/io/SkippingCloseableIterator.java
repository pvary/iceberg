/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io;

import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public interface SkippingCloseableIterator<T> extends CloseableIterator<T> {
  void skipTo(long position);

  long position();

  static <E> SkippingCloseableIterator<E> wrap(CloseableIterator<E> iterator) {
    return new SkippingCloseableIterator<>() {
      private long position = 0;

      @Override
      public void skipTo(long targetPosition) {
        Preconditions.checkArgument(
            targetPosition >= position,
            "Cannot skip backwards: current position %s, target position %s",
            position,
            targetPosition);
        while (position < targetPosition && iterator.hasNext()) {
          iterator.next();
          position++;
        }
      }

      @Override
      public long position() {
        return position;
      }

      @Override
      public void close() throws IOException {
        iterator.close();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public E next() {
        E next = iterator.next();
        position++;
        return next;
      }
    };
  }
}
