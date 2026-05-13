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
package org.apache.iceberg.index;

import java.util.Arrays;
import org.apache.iceberg.util.BucketUtil;

/**
 * Simple utility that distributes sequential row ids into a power-of-two number of buckets using
 * Iceberg's MurmurHash-based bucket hash, and prints the resulting distribution statistics
 * (min/max/avg bucket size, plus standard deviation and a few percentiles).
 *
 * <p>Usage: {@code HashCheck <numRows> <expectedBucketSize>}
 *
 * <p>The number of buckets is computed as the power of two nearest (in log2 space) to {@code
 * numRows / expectedBucketSize}.
 */
public final class HashCheck {

  private HashCheck() {}

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: HashCheck <numRows> <expectedBucketSize>");
      System.exit(1);
    }

    long numRows = Long.parseLong(args[0]);
    long expectedBucketSize = Long.parseLong(args[1]);

    if (numRows <= 0) {
      throw new IllegalArgumentException("numRows must be > 0, was: " + numRows);
    }
    if (expectedBucketSize <= 0) {
      throw new IllegalArgumentException(
          "expectedBucketSize must be > 0, was: " + expectedBucketSize);
    }

    int numBuckets = nearestPowerOfTwo(Math.max(1L, numRows / expectedBucketSize));

    System.out.printf("Rows ................: %,d%n", numRows);
    System.out.printf("Expected bucket size : %,d%n", expectedBucketSize);
    System.out.printf("Bucket count (2^n) ..: %,d (2^%d)%n", numBuckets, log2(numBuckets));
    System.out.printf("Avg rows / bucket ...: %.2f%n", numRows / (double) numBuckets);

    long[] counts = new long[numBuckets];
    int mask = numBuckets - 1; // numBuckets is a power of two

    long start = System.nanoTime();
    for (long id = 0; id < numRows; id++) {
      // Mask the sign bit the same way Iceberg's bucket transform does, then map to a bucket.
      int hash = BucketUtil.hash(id) & Integer.MAX_VALUE;
      counts[hash & mask]++;
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    printStats(counts, numRows, elapsedMs);
  }

  private static void printStats(long[] counts, long numRows, long elapsedMs) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long emptyBuckets = 0;
    double mean = numRows / (double) counts.length;
    double sqSum = 0.0;
    for (long c : counts) {
      if (c < min) {
        min = c;
      }
      if (c > max) {
        max = c;
      }
      if (c == 0) {
        emptyBuckets++;
      }
      double diff = c - mean;
      sqSum += diff * diff;
    }
    double stddev = Math.sqrt(sqSum / counts.length);

    long[] sorted = counts.clone();
    Arrays.sort(sorted);

    System.out.println();
    System.out.println("--- Distribution ---");
    System.out.printf("Min .................: %,d%n", min);
    System.out.printf("Max .................: %,d%n", max);
    System.out.printf("Mean ................: %.2f%n", mean);
    System.out.printf(
        "Stddev ..............: %.2f (%.2f%% of mean)%n", stddev, 100.0 * stddev / mean);
    System.out.printf("p50 .................: %,d%n", percentile(sorted, 50));
    System.out.printf("p95 .................: %,d%n", percentile(sorted, 95));
    System.out.printf("p99 .................: %,d%n", percentile(sorted, 99));
    System.out.printf(
        "Empty buckets .......: %,d (%.2f%%)%n",
        emptyBuckets, 100.0 * emptyBuckets / counts.length);
    System.out.printf("Max / Mean ratio ....: %.3f%n", max / mean);
    System.out.printf("Elapsed .............: %,d ms%n", elapsedMs);
  }

  private static long percentile(long[] sorted, int p) {
    int idx = (int) Math.min(sorted.length - 1L, Math.round((p / 100.0) * (sorted.length - 1)));
    return sorted[idx];
  }

  /** Returns the power of two nearest (in log2 / geometric space) to the given positive value. */
  static int nearestPowerOfTwo(long value) {
    if (value < 1) {
      return 1;
    }
    if (value >= (1L << 30)) {
      return 1 << 30;
    }
    int high = 32 - Integer.numberOfLeadingZeros((int) (value - 1));
    if (high < 1) {
      return 1;
    }
    long highVal = 1L << high;
    long lowVal = 1L << (high - 1);
    // value is closer to highVal (in geometric/log2 space) iff value*value >= highVal*lowVal.
    return (value * value >= highVal * lowVal) ? (int) highVal : (int) lowVal;
  }

  private static int log2(int powerOfTwo) {
    return Integer.numberOfTrailingZeros(powerOfTwo);
  }
}
