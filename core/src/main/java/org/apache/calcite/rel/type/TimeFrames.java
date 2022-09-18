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
import org.apache.calcite.util.TimestampString;

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
    b.addCore(TimeUnit.SECOND);
    b.addSub(TimeUnit.MINUTE, false, 60, TimeUnit.SECOND);
    b.addSub(TimeUnit.HOUR, false, 60, TimeUnit.MINUTE);
    b.addSub(TimeUnit.DAY, false, 24, TimeUnit.HOUR);
    b.addSub(TimeUnit.WEEK, false, 7, TimeUnit.DAY,
        new TimestampString(1970, 1, 5, 0, 0, 0));
    b.addSub(TimeUnit.MILLISECOND, true, 1_000, TimeUnit.SECOND);
    b.addSub(TimeUnit.MICROSECOND, true, 1_000, TimeUnit.MILLISECOND);
    b.addSub(TimeUnit.NANOSECOND, true, 1_000, TimeUnit.MICROSECOND);

    b.addCore(TimeUnit.MONTH);
    b.addSub(TimeUnit.QUARTER, false, 3, TimeUnit.MONTH);
    b.addSub(TimeUnit.YEAR, false, 12, TimeUnit.MONTH);
    b.addSub(TimeUnit.DECADE, false, 10, TimeUnit.YEAR);
    b.addSub(TimeUnit.CENTURY, false, 100, TimeUnit.YEAR,
        new TimestampString(2001, 1, 1, 0, 0, 0));
    b.addSub(TimeUnit.MILLENNIUM, false, 1_000, TimeUnit.YEAR,
        new TimestampString(2001, 1, 1, 0, 0, 0));

    b.addRollup(TimeUnit.DAY, TimeUnit.MONTH);

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
    void addCore(TimeUnit unit) {
      super.addCore(unit.name());
    }

    void addSub(TimeUnit unit, boolean divide, Number count,
        TimeUnit baseUnit) {
      addSub(unit, divide, count, baseUnit, TimestampString.EPOCH);
    }

    void addSub(TimeUnit unit, boolean divide, Number count,
        TimeUnit baseUnit, TimestampString epoch) {
      addSub(unit.name(), divide, count, baseUnit.name(), epoch);
    }

    void addRollup(TimeUnit fromUnit, TimeUnit toUnit) {
      addRollup(fromUnit.name(), toUnit.name());
    }
  }
}
