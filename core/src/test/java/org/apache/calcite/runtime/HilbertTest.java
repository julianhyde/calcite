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

import org.junit.Ignore;
import org.junit.Test;

import java.util.Locale;

import static org.apache.calcite.runtime.Hilbert.bit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link Hilbert}.
 */
public class HilbertTest {
  @Test public void testInterleave() {
    assertThat(Hilbert.interleave(new long[]{0, 0}, 2), is(0L));
    assertThat(Hilbert.interleave(new long[]{1, 0}, 2), is(1L));
    assertThat(Hilbert.interleave(new long[]{0, 1}, 2), is(2L));
    assertThat(Hilbert.interleave(new long[]{1, 1}, 2), is(3L));
    assertThat(Hilbert.interleave(new long[]{15, 0}, 4), is(85L)); // 01010101
    assertThat(Hilbert.interleave(new long[]{0, 15}, 4), is(170L)); // 10101010
  }

  @Test public void testCoordsToCurve() {
    assertThat(Hilbert.coordsToCurve(0, 0, 0), is(0L));
    assertThat(Hilbert.coordsToCurve(0, 0, 1), is(0L));
    assertThat(Hilbert.coordsToCurve(0, 0, 2), is(0L));
  }

  @Ignore
  @Test public void testSpace1() {
    final String space = ""
        + "   1   2\n"
        + "   0   3\n";
    assertThat(Hilbert.space(1), is(space));
  }

  @Ignore
  @Test public void testSpace2() {
    final String space = ""
        + "   5   6   9  10\n"
        + "   4   7   8  11\n"
        + "   3   2  13  12\n"
        + "   0   1  14  15\n";
    assertThat(Hilbert.space(2), is(space));
  }

  /** This test was in the original paper referenced from
   * {@link Hilbert#transposeToAxes(long[], int)}. */
  @Test public void testHilbert2() {
    final long[] x = {5, 10, 20}; // any position in 32x32x32 cube
    // Hilbert transpose for 5 bits and 3 dimensions
    Hilbert.axesToTranspose(x, 5);
    // 001111010111001 binary equals 7865 decimal
    final String s = String.format(Locale.ROOT,
        "%d%d%d%d%d%d%d%d%d%d%d%d%d%d%d",
        bit(x, 0, 4), bit(x, 1, 4), bit(x, 2, 4), bit(x, 0, 3), bit(x, 1, 3),
        bit(x, 2, 3), bit(x, 0, 2), bit(x, 1, 2), bit(x, 2, 2), bit(x, 0, 1),
        bit(x, 1, 1), bit(x, 2, 1), bit(x, 0, 0), bit(x, 1, 0), bit(x, 2, 0));
    assertThat(s, is("001111010111001"));

    // and back again
    Hilbert.transposeToAxes(x, 5);
    assertThat(x[0], is(5L));
    assertThat(x[1], is(10L));
    assertThat(x[2], is(20L));
  }

}

// End HilbertTest.java
