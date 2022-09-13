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
package org.apache.calcite.rel.type;

import org.apache.calcite.avatica.util.TimeUnit;

import org.apache.commons.math3.fraction.BigFraction;

import static java.util.Objects.requireNonNull;

/** Utilities for {@link TimeFrame}. */
public class TimeFrames {
  private TimeFrames() {
  }

  private static final TimeFrameSet AVATICA = map();

  /** Returns a time frame for an Avatica time unit. */
  public static TimeFrame of(TimeUnit unit) {
    return requireNonNull(AVATICA.get(unit.name()));
  }

  /** Returns a map from Avatica time units to time frames. */
  public static TimeFrameSet map() {
    final MyBuilder b = new MyBuilder();
    b.addCore(TimeUnit.SECOND)
        .addMultiple(TimeUnit.MINUTE, 60, TimeUnit.SECOND)
        .addMultiple(TimeUnit.HOUR, 60, TimeUnit.MINUTE)
        .addMultiple(TimeUnit.DAY, 24, TimeUnit.HOUR)
        .addMultiple(TimeUnit.WEEK, 7, TimeUnit.DAY)
        .addDivision(TimeUnit.MILLISECOND, 1_000, TimeUnit.SECOND)
        .addDivision(TimeUnit.MICROSECOND, 1_000, TimeUnit.MILLISECOND)
        .addDivision(TimeUnit.NANOSECOND, 1_000, TimeUnit.MICROSECOND)

        .addCore(TimeUnit.MONTH)
        .addMultiple(TimeUnit.YEAR, 12, TimeUnit.MONTH)
        .addMultiple(TimeUnit.DECADE, 10, TimeUnit.YEAR)
        .addMultiple(TimeUnit.CENTURY, 100, TimeUnit.YEAR)
        .addMultiple(TimeUnit.MILLENNIUM, 1_000, TimeUnit.YEAR);

    // Avatica time units:

    // ISOYEAR
    // DOW
    // ISODOW
    // DOY
    // EPOCH

    // Other time units:
    // HALF_DAY

    // Looker time units:
    // QUARTER
    // RAW

    // hourX, X in [2, 3, 4, 6, 8, 12], e.g. hour6 is a 6-hour segment.
    // For example, a row with a time of '2014-09-01 08:03:17' would have an
    // hour6 of '2014-09-01 06:00:00'.

    // minuteX, X in [2, 3, 4, 5, 6, 10, 12, 15, 20, or 30]

    // secondX,

    // millisecondX, X in [2, 4, 5, 8, 10, 20, 25, 40, 50, 100, 125, 200, 250,
    // 500].

    // date, e.g. 2017-09-03

    // week
    // day_of_week, e.g. Monday
    // day_of_week_index, e.g. 0 (Monday), 6 (Saturday)

    // month, e.g. 2014-09
    // month_num, e.g. 9
    // fiscal_month_num, e.g. 9
    // month_name, e.g. September
    // day_of_month, e.g. 3

    // year, e.g. 2017
    // fiscal_year, e.g. FY2017
    // day_of_year, e.g. 143
    // week_of_year, e.g. 17

    return b.build();
  }

  /** Specialization of {@link org.apache.calcite.rel.type.TimeFrameSet.Builder}
   * for Avatica's built-in time frames. */
  private static class MyBuilder extends TimeFrameSet.Builder {
    public MyBuilder addCore(TimeUnit unit) {
      super.addCore(unit.name());
      return this;
    }

    MyBuilder addMultiple(TimeUnit unit, Number count, TimeUnit baseUnit) {
      super.addMultiple(unit.name(), count, baseUnit.name());
      return this;
    }

    MyBuilder addDivision(TimeUnit unit, Number count, TimeUnit baseUnit) {
      final BigFraction f = toFraction(count);
      super.addMultiple(unit.name(), BigFraction.ONE.divide(f), baseUnit.name());
      return this;
    }
  }
}
