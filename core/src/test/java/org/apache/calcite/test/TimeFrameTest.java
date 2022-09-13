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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.rel.type.TimeFrames;

import org.apache.commons.math3.fraction.BigFraction;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/** Unit test for {@link org.apache.calcite.rel.type.TimeFrame}. */
public class TimeFrameTest {
  /** Unit test for
   * {@link org.apache.calcite.rel.type.TimeFrames#of(TimeUnit)}. */
  @Test void testAvaticaTimeFrame() {
    final TimeFrame year = TimeFrames.of(TimeUnit.YEAR);
    assertThat(year, notNullValue());
    assertThat(year.name(), is("YEAR"));

    final TimeFrame month = TimeFrames.of(TimeUnit.MONTH);
    assertThat(month, notNullValue());
    assertThat(month.name(), is("MONTH"));

    final Number monthPerYear = month.per(year);
    assertThat(monthPerYear, notNullValue());
    assertThat(monthPerYear, is(new BigFraction(12)));
    final Number yearPerMonth = year.per(month);
    assertThat(yearPerMonth, notNullValue());
    assertThat(yearPerMonth, is(BigFraction.ONE.divide(12)));
    final Number monthPerMonth = month.per(month);
    assertThat(monthPerMonth, notNullValue());
    assertThat(monthPerMonth, is(BigFraction.ONE));

    final TimeFrame second = TimeFrames.of(TimeUnit.SECOND);
    assertThat(second, notNullValue());
    assertThat(second.name(), is("SECOND"));

    final TimeFrame minute = TimeFrames.of(TimeUnit.MINUTE);
    assertThat(minute, notNullValue());
    assertThat(minute.name(), is("MINUTE"));

    final TimeFrame nano = TimeFrames.of(TimeUnit.NANOSECOND);
    assertThat(nano, notNullValue());
    assertThat(nano.name(), is("NANOSECOND"));

    final Number secondPerMonth = second.per(month);
    assertThat(secondPerMonth, nullValue());
    final Number nanoPerMinute = nano.per(minute);
    assertThat(nanoPerMinute, notNullValue());
    assertThat(nanoPerMinute,
        is(BigFraction.ONE.multiply(1_000).multiply(1_000).multiply(1_000)
            .multiply(60)));
  }
}
