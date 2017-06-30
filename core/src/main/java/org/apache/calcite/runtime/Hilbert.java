/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import com.google.common.annotations.VisibleForTesting;

import java.util.Formatter;
import java.util.Locale;

/**
 * Utilities for Hilbert space-filling curves.
 */
public class Hilbert {
  private Hilbert() {}

  public static long coordsToCurve(int x, int y, int bitCount) {
    assert bitCount < 32;
    final long[] coords = {x, y};
    axesToTranspose(coords, bitCount);
    return interleave(coords, bitCount);
  }

  @VisibleForTesting
  static long interleave(long[] coords, int bitCount) {
    long x = 0;
    for (int b = 0; b < bitCount; b++) {
      for (int i = 0; i < coords.length; i++) {
        long coord = coords[i];
        if ((coord & (1 << b)) != 0) {
          x |= 1 << (b * coords.length + i);
        }
      }
    }
    return x;
  }

  /** Prints a 2-dimensional Hilbert curve with a given number of bits per
   * axis. For example, {@code space(1)} prints
   *
   * <blockquote>
   *   <pre>
   *   1   2
   *   0   3
   *   </pre>
   * </blockquote>
   *
   * <p>and {@code space(2)} prints
   *
   * <blockquote>
   *   <pre>
   *   5   6   9  10
   *   4   7   8  11
   *   3   2  13  12
   *   0   1  14  15
   *   </pre>
   * </blockquote>
   *
   * @param bitCount Number of bits per dimension
   */
  @VisibleForTesting
  static String space(int bitCount) {
    assert bitCount > 0;
    final int max = 1 << bitCount;
    final StringBuilder b = new StringBuilder();
    final Formatter f = new Formatter(b, Locale.ROOT);
    for (int y = max - 1; y >= 0; y--) {
      for (int x = 0; x < max; x++) {
        final long z = coordsToCurve(x, y, bitCount);
        f.format("%4d", z);
      }
      f.format("%n");
    }
    f.flush();
    return b.toString();
  }

  /**
   * Transforms in-place from Hilbert transpose to geometrical axes.
   *
   * <p>Example: b=5 bits for each of n=3 coordinates.
   * 15-bit Hilbert integer = A B C D E F G H I J K L M N O is stored
   * as its Transpose:
   *
   * <blockquote>
   *   <pre>
   *     X[0] = A D G J M               X[2]|
   *     X[1] = B E H K N         <-------> | /X[1]
   *     X[2] = C F I L O              axes |/
   *            high low                    0------ X[0]
   *   </pre>
   * </blockquote>
   *
   * <p>Axes are stored conventionally as b-bit integers.
   *
   * <p>Based on public domain code in "Programming the Hilbert curve"
   * by John Skilling, <a href="http://doi.org/10.1063/1.1751381">AIP
   * Conference Proceedings 707, 381 (2004)</a>.
   *
   * @param x Position (co-ordinates in n dimensions)
   * @param b Number of bits
   */
  public static void transposeToAxes(long[] x, int b) {
    final int n = x.length; // dimension
    final long m = 2 << (b - 1);
    // Gray decode by H ^ (H / 2)
    long t = x[n - 1] >> 1;
    // The original paper wrongly had ">=" rather than ">" next line.
    for (int i = n - 1; i > 0; i--) {
      x[i] ^= x[i - 1];
    }
    x[0] ^= t;
    // Undo excess work
    for (long q = 2; q != m; q <<= 1) {
      long p = q - 1;
      for (int i = n - 1; i >= 0; i--) {
        if ((x[i] & q) != 0) {
          x[0] ^= p; // invert
        } else {
          // exchange
          t = (x[0] ^ x[i]) & p;
          x[0] ^= t;
          x[i] ^= t;
        }
      }
    }
  }

  /**
   * Transforms in-place from geometrical axes to Hilbert transpose.
   *
   * @param x Position
   * @param b Number of bits
   * @see #transposeToAxes(long[], int)
   */
  public static void axesToTranspose(long[] x, int b) {
    final int n = x.length; // dimension
    final long m = 1 << (b - 1);
    // Inverse undo
    for (long q = m; q > 1; q >>= 1) {
      long p = q - 1;
      for (int i = 0; i < n; i++) {
        if ((x[i] & q) != 0) {
          x[0] ^= p; // invert
        } else {
          // exchange
          final long t = (x[0] ^ x[i]) & p;
          x[0] ^= t;
          x[i] ^= t;
        }
      }
    }
    // Gray encode
    for (int i = 1; i < n; i++) {
      x[i] ^= x[i - 1];
    }
    long t = 0;
    for (long q = m; q > 1; q >>= 1) {
      if ((x[n - 1] & q) != 0L) {
        t ^= q - 1;
      }
    }
    for (int i = 0; i < n; i++) {
      x[i] ^= t;
    }
  }

  @VisibleForTesting
  static int bit(long[] x, int w, int bit) {
    return (int) ((x[w] >> bit) & 1);
  }
}

// End Hilbert.java
