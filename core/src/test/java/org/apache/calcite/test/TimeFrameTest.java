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
import org.apache.calcite.rel.type.TimeFrameSet;
import org.apache.calcite.rel.type.TimeFrames;

import org.apache.commons.math3.fraction.BigFraction;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.avatica.util.DateTimeUtils.dateStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.timestampStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixDateToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestampToString;
import static org.apache.calcite.avatica.util.TimeUnit.CENTURY;
import static org.apache.calcite.avatica.util.TimeUnit.DAY;
import static org.apache.calcite.avatica.util.TimeUnit.DECADE;
import static org.apache.calcite.avatica.util.TimeUnit.HOUR;
import static org.apache.calcite.avatica.util.TimeUnit.MICROSECOND;
import static org.apache.calcite.avatica.util.TimeUnit.MILLENNIUM;
import static org.apache.calcite.avatica.util.TimeUnit.MILLISECOND;
import static org.apache.calcite.avatica.util.TimeUnit.MINUTE;
import static org.apache.calcite.avatica.util.TimeUnit.MONTH;
import static org.apache.calcite.avatica.util.TimeUnit.NANOSECOND;
import static org.apache.calcite.avatica.util.TimeUnit.QUARTER;
import static org.apache.calcite.avatica.util.TimeUnit.SECOND;
import static org.apache.calcite.avatica.util.TimeUnit.WEEK;
import static org.apache.calcite.avatica.util.TimeUnit.YEAR;

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

    final TimeFrame month = TimeFrames.of(MONTH);
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

  @Test void testEvalFloor() {
    final Fixture f = new Fixture();
    f.checkDateFloor("1970-03-04", WEEK, is("1970-03-02"));
    f.checkDateFloor("1970-03-03", WEEK, is("1970-03-02"));
    f.checkDateFloor("1970-03-02", WEEK, is("1970-03-02"));
    f.checkDateFloor("1970-03-01", WEEK, is("1970-02-23"));

    f.checkTimestampFloor("1970-01-01 01:23:45", HOUR,
        0, is("1970-01-01 01:00:00"));
    f.checkTimestampFloor("1970-01-01 01:23:45", MINUTE,
        0, is("1970-01-01 01:23:00"));
    f.checkTimestampFloor("1970-01-01 01:23:45.67", SECOND,
        0, is("1970-01-01 01:23:45"));
    f.checkTimestampFloor("1970-01-01 01:23:45.6789012345", MILLISECOND,
        4, is("1970-01-01 01:23:45.6790"));
    // Time frames can represent unlimited precision, but out representation of
    // timestamp can't represent more than millisecond precision.
    f.checkTimestampFloor("1970-01-01 01:23:45.6789012345", MICROSECOND,
        7, is("1970-01-01 01:23:45.6790000"));

    f.checkTimestampFloor("1971-12-25 01:23:45", DAY,
        0, is("1971-12-25 00:00:00"));
    f.checkTimestampFloor("1971-12-25 01:23:45", WEEK,
        0, is("1971-12-20 00:00:00"));
  }

  @Test void testCanRollUp() {
    final Fixture f = new Fixture();

    // The rollup from DAY to MONTH is special. It provides the bridge between
    // the frames in the SECOND family and those in the MONTH family.
    f.checkCanRollUp(DAY, MONTH, true);
    f.checkCanRollUp(MONTH, DAY, false);

    f.checkCanRollUp(NANOSECOND, NANOSECOND, true);
    f.checkCanRollUp(NANOSECOND, MICROSECOND, true);
    f.checkCanRollUp(NANOSECOND, MILLISECOND, true);
    f.checkCanRollUp(NANOSECOND, SECOND, true);
    f.checkCanRollUp(NANOSECOND, MINUTE, true);
    f.checkCanRollUp(NANOSECOND, HOUR, true);
    f.checkCanRollUp(NANOSECOND, DAY, true);
    f.checkCanRollUp(NANOSECOND, WEEK, true);
    f.checkCanRollUp(NANOSECOND, MONTH, true);
    f.checkCanRollUp(NANOSECOND, QUARTER, true);
    f.checkCanRollUp(NANOSECOND, YEAR, true);
    f.checkCanRollUp(NANOSECOND, CENTURY, true);
    f.checkCanRollUp(NANOSECOND, DECADE, true);
    f.checkCanRollUp(NANOSECOND, MILLENNIUM, true);

    f.checkCanRollUp(MICROSECOND, NANOSECOND, false);
    f.checkCanRollUp(MICROSECOND, MICROSECOND, true);
    f.checkCanRollUp(MICROSECOND, MILLISECOND, true);
    f.checkCanRollUp(MICROSECOND, SECOND, true);
    f.checkCanRollUp(MICROSECOND, MINUTE, true);
    f.checkCanRollUp(MICROSECOND, HOUR, true);
    f.checkCanRollUp(MICROSECOND, DAY, true);
    f.checkCanRollUp(MICROSECOND, WEEK, true);
    f.checkCanRollUp(MICROSECOND, MONTH, true);
    f.checkCanRollUp(MICROSECOND, QUARTER, true);
    f.checkCanRollUp(MICROSECOND, YEAR, true);
    f.checkCanRollUp(MICROSECOND, CENTURY, true);
    f.checkCanRollUp(MICROSECOND, DECADE, true);
    f.checkCanRollUp(MICROSECOND, MILLENNIUM, true);

    f.checkCanRollUp(MILLISECOND, NANOSECOND, false);
    f.checkCanRollUp(MILLISECOND, MICROSECOND, false);
    f.checkCanRollUp(MILLISECOND, MILLISECOND, true);
    f.checkCanRollUp(MILLISECOND, SECOND, true);
    f.checkCanRollUp(MILLISECOND, MINUTE, true);
    f.checkCanRollUp(MILLISECOND, HOUR, true);
    f.checkCanRollUp(MILLISECOND, DAY, true);
    f.checkCanRollUp(MILLISECOND, WEEK, true);
    f.checkCanRollUp(MILLISECOND, MONTH, true);
    f.checkCanRollUp(MILLISECOND, QUARTER, true);
    f.checkCanRollUp(MILLISECOND, YEAR, true);
    f.checkCanRollUp(MILLISECOND, CENTURY, true);
    f.checkCanRollUp(MILLISECOND, DECADE, true);
    f.checkCanRollUp(MILLISECOND, MILLENNIUM, true);

    f.checkCanRollUp(SECOND, NANOSECOND, false);
    f.checkCanRollUp(SECOND, MICROSECOND, false);
    f.checkCanRollUp(SECOND, MILLISECOND, false);
    f.checkCanRollUp(SECOND, SECOND, true);
    f.checkCanRollUp(SECOND, MINUTE, true);
    f.checkCanRollUp(SECOND, HOUR, true);
    f.checkCanRollUp(SECOND, DAY, true);
    f.checkCanRollUp(SECOND, WEEK, true);
    f.checkCanRollUp(SECOND, MONTH, true);
    f.checkCanRollUp(SECOND, QUARTER, true);
    f.checkCanRollUp(SECOND, YEAR, true);
    f.checkCanRollUp(SECOND, CENTURY, true);
    f.checkCanRollUp(SECOND, DECADE, true);
    f.checkCanRollUp(SECOND, MILLENNIUM, true);

    f.checkCanRollUp(MINUTE, NANOSECOND, false);
    f.checkCanRollUp(MINUTE, MICROSECOND, false);
    f.checkCanRollUp(MINUTE, MILLISECOND, false);
    f.checkCanRollUp(MINUTE, SECOND, false);
    f.checkCanRollUp(MINUTE, MINUTE, true);
    f.checkCanRollUp(MINUTE, HOUR, true);
    f.checkCanRollUp(MINUTE, DAY, true);
    f.checkCanRollUp(MINUTE, WEEK, true);
    f.checkCanRollUp(MINUTE, MONTH, true);
    f.checkCanRollUp(MINUTE, QUARTER, true);
    f.checkCanRollUp(MINUTE, YEAR, true);
    f.checkCanRollUp(MINUTE, CENTURY, true);
    f.checkCanRollUp(MINUTE, DECADE, true);
    f.checkCanRollUp(MINUTE, MILLENNIUM, true);

    f.checkCanRollUp(HOUR, NANOSECOND, false);
    f.checkCanRollUp(HOUR, MICROSECOND, false);
    f.checkCanRollUp(HOUR, MILLISECOND, false);
    f.checkCanRollUp(HOUR, SECOND, false);
    f.checkCanRollUp(HOUR, MINUTE, false);
    f.checkCanRollUp(HOUR, HOUR, true);
    f.checkCanRollUp(HOUR, DAY, true);
    f.checkCanRollUp(HOUR, WEEK, true);
    f.checkCanRollUp(HOUR, MONTH, true);
    f.checkCanRollUp(HOUR, QUARTER, true);
    f.checkCanRollUp(HOUR, YEAR, true);
    f.checkCanRollUp(HOUR, DECADE, true);
    f.checkCanRollUp(HOUR, CENTURY, true);
    f.checkCanRollUp(HOUR, MILLENNIUM, true);

    f.checkCanRollUp(DAY, NANOSECOND, false);
    f.checkCanRollUp(DAY, MICROSECOND, false);
    f.checkCanRollUp(DAY, MILLISECOND, false);
    f.checkCanRollUp(DAY, SECOND, false);
    f.checkCanRollUp(DAY, MINUTE, false);
    f.checkCanRollUp(DAY, HOUR, false);
    f.checkCanRollUp(DAY, DAY, true);
    f.checkCanRollUp(DAY, WEEK, true);
    f.checkCanRollUp(DAY, MONTH, true);
    f.checkCanRollUp(DAY, QUARTER, true);
    f.checkCanRollUp(DAY, YEAR, true);
    f.checkCanRollUp(DAY, DECADE, true);
    f.checkCanRollUp(DAY, CENTURY, true);
    f.checkCanRollUp(DAY, MILLENNIUM, true);

    f.checkCanRollUp(WEEK, NANOSECOND, false);
    f.checkCanRollUp(WEEK, MICROSECOND, false);
    f.checkCanRollUp(WEEK, MILLISECOND, false);
    f.checkCanRollUp(WEEK, SECOND, false);
    f.checkCanRollUp(WEEK, MINUTE, false);
    f.checkCanRollUp(WEEK, HOUR, false);
    f.checkCanRollUp(WEEK, DAY, false);
    f.checkCanRollUp(WEEK, WEEK, true);
    f.checkCanRollUp(WEEK, MONTH, false); //  <-- important!
    f.checkCanRollUp(WEEK, QUARTER, false);
    f.checkCanRollUp(WEEK, YEAR, false);
    f.checkCanRollUp(WEEK, DECADE, false);
    f.checkCanRollUp(WEEK, CENTURY, false);
    f.checkCanRollUp(WEEK, MILLENNIUM, false);
  }

  /** Test fixture. Contains everything you need to write fluent tests. */
  static class Fixture {
    final TimeFrameSet timeFrameSet = TimeFrames.map();

    void checkDateFloor(String in, TimeUnit unit, Matcher<String> matcher) {
      int inDate = dateStringToUnixDate(in);
      int outDate = timeFrameSet.floorDate(inDate, timeFrameSet.get(unit));
      assertThat(in, unixDateToString(outDate), matcher);
    }

    void checkTimestampFloor(String in, TimeUnit unit, int precision,
        Matcher<String> matcher) {
      long inTs = timestampStringToUnixDate(in);
      long outTs = timeFrameSet.floorTimestamp(inTs, timeFrameSet.get(unit));
      assertThat(in, unixTimestampToString(outTs, precision), matcher);
    }

    void checkCanRollUp(TimeUnit fromUnit, TimeUnit toUnit, boolean can) {
      TimeFrame fromFrame = timeFrameSet.get(fromUnit);
      TimeFrame toFrame = timeFrameSet.get(toUnit);
      if (can) {
        assertThat("can roll up " + fromUnit + " to " + toUnit,
            fromFrame.canRollUpTo(toFrame),
            is(true));

        // The 'canRollUpTo' method should be a partial order.
        // A partial order is reflexive (for all x, x = x)
        // and antisymmetric (for all x, y, if x <= y and x != y, then !(y <= x))
        assertThat(toFrame.canRollUpTo(fromFrame), is(fromUnit == toUnit));
      } else {
        assertThat("can roll up " + fromUnit + " to " + toUnit,
            fromFrame.canRollUpTo(toFrame),
            is(false));
      }
    }
  }
}
