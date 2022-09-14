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

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.util.TimestampString;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Set of {@link TimeFrame} definitions. */
public class TimeFrameSet {
  private final ImmutableMap<String, TimeFrame> map;

  private TimeFrameSet(ImmutableMap<String, TimeFrame> map) {
    this.map = requireNonNull(map, "map");
  }

  /** Creates a Builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the time frame with the given name,
   * or throws {@link NullPointerException}. */
  public TimeFrame get(String name) {
    return requireNonNull(map.get(name),
        () -> "not found: " + name);
  }

  /** Returns the time frame with the given name,
   * or throws {@link NullPointerException}. */
  public TimeFrame get(TimeUnit timeUnit) {
    return get(timeUnit.name());
  }

  /** Computes "FLOOR(date TO frame)", where {@code date} is the number of
   * days since UNIX Epoch. */
  public int floorDate(int date, TimeFrame frame) {
    final TimeFrame dayFrame = get(TimeUnit.DAY);
    final BigFraction f = frame.per(dayFrame);
    if (f != null
        && f.getNumerator().equals(BigInteger.ONE)) {
      final int m = f.getDenominator().intValueExact(); // 7 for WEEK
      final long mod =
          DateTimeUtils.floorMod(date - frame.dateEpoch(), m);
      return date - (int) mod;
    }
    return date;
  }

  /** Computes "FLOOR(timestamp TO frame)", where {@code date} is the number of
   * milliseconds since UNIX Epoch. */
  public long floorTimestamp(long ts, TimeFrame frame) {
    final TimeFrame secondFrame = get(TimeUnit.MILLISECOND.name());
    final BigFraction f = frame.per(secondFrame);
    if (f != null
        && f.getNumerator().equals(BigInteger.ONE)) {
      final long m = f.getDenominator().longValue(); // 60,000 for MINUTE
      final long mod =
          DateTimeUtils.floorMod(ts - frame.timestampEpoch(), m);
      return ts - mod;
    }
    return ts;
  }

  /** Builds a collection of time frames. */
  public static class Builder {
    Builder() {
    }

    final Map<String, TimeFrameImpl> map = new LinkedHashMap<>();

    public TimeFrameSet build() {
      return new TimeFrameSet(ImmutableMap.copyOf(map));
    }

    /** Converts a number to an exactly equivalent {@code BigInteger}.
     * May silently lose precision if n is a {@code Float} or {@code Double}. */
    static BigInteger toBigInteger(Number number) {
      return number instanceof BigInteger ? (BigInteger) number
          : BigInteger.valueOf(number.longValue());
    }

    public Builder addCore(String name) {
      map.put(name, new CoreTimeFrame(name));
      return this;
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    Builder addSub(String name, boolean divide, Number count,
        String baseName, TimestampString epoch) {
      final TimeFrameImpl baseFrame = map.get(baseName);
      map.put(name,
          new SubTimeFrame(name, baseFrame, divide, toBigInteger(count),
              epoch));
      return this;
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    public Builder addMultiple(String name, Number count,
        String baseName) {
      return addSub(name, false, count, baseName, TimestampString.EPOCH);
    }

    /** Defines such that each {@code baseUnit} consists of {@code count}
     * instances of the new unit. */
    public Builder addDivision(String name, Number count, String baseName) {
      return addSub(name, true, count, baseName, TimestampString.EPOCH);
    }

    /** Adds all time frames in {@code timeFrameSet} to this Builder. */
    public Builder addAll(TimeFrameSet timeFrameSet) {
      timeFrameSet.map.values().forEach(frame ->
          ((TimeFrameImpl) frame).replicate(this));
      return this;
    }

    /** Replaces the epoch of the most recently added frame. */
    public Builder withEpoch(TimestampString epoch) {
      final String name = Iterables.getLast(map.keySet());
      final SubTimeFrame value =
          requireNonNull((SubTimeFrame) map.remove(name));
      return value.replicateWithEpoch(this, epoch);
    }
  }

  /** Implementation of {@link TimeFrame}. */
  abstract static class TimeFrameImpl implements TimeFrame {
    final String name;

    TimeFrameImpl(String name) {
      this.name = requireNonNull(name, "name");
    }

    @Override public String toString() {
      return name;
    }

    @Override public String name() {
      return name;
    }

    @Override public @Nullable BigFraction per(TimeFrame timeFrame) {
      final Map<TimeFrame, BigFraction> map = new HashMap<>();
      final Map<TimeFrame, BigFraction> map2 = new HashMap<>();
      expand(map, BigFraction.ONE);
      ((TimeFrameImpl) timeFrame).expand(map2, BigFraction.ONE);
      for (Map.Entry<TimeFrame, BigFraction> entry : map.entrySet()) {
        // We assume that if there are multiple units in common, the multipliers
        // are the same for all.
        //
        // If they are not, it will be because the user defined the units
        // inconsistently. TODO: check for that in the builder.
        final BigFraction value2 = map2.get(entry.getKey());
        if (value2 != null) {
          return value2.divide(entry.getValue());
        }
      }
      return null;
    }

    protected void expand(Map<TimeFrame, BigFraction> map, BigFraction f) {
      map.put(this, f);
    }

    /** Adds a time frame like this to a builder. */
    abstract Builder replicate(Builder b);
  }

  /** Core time frame (such as SECOND and MONTH). */
  static class CoreTimeFrame extends TimeFrameImpl {
    CoreTimeFrame(String name) {
      super(name);
    }

    @Override Builder replicate(Builder b) {
      b.addCore(name);
      return b;
    }
  }

  /** A time frame is composed of another time frame.
   *
   * <p>For example, {@code MINUTE} is composed of 60 {@code SECOND};
   * (factor = 60, divide = false);
   * {@code MILLISECOND} is composed of 1 / 1000 {@code SECOND}
   * (factor = 1000, divide = true).
   *
   * <p>A sub-time frame S is aligned with its parent frame P;
   * that is, every instance of S belongs to one instance of P.
   * Every {@code MINUTE} belongs to one {@code HOUR};
   * not every {@code WEEK} belongs to precisely one {@code MONTH} or
   * {@code MILLENNIUM}.
   */
  static class SubTimeFrame extends TimeFrameImpl {
    private final TimeFrameImpl base;
    private final boolean divide;
    private final BigInteger multiplier;
    private final TimestampString epoch;

    SubTimeFrame(String name, TimeFrameImpl base, boolean divide,
        BigInteger multiplier, TimestampString epoch) {
      super(name);
      this.base = requireNonNull(base, "base");
      this.divide = divide;
      this.multiplier = requireNonNull(multiplier, "multiplier");
      this.epoch = requireNonNull(epoch, "epoch");
    }

    @Override public String toString() {
      return name + ", composedOf " + multiplier + " " + base.name;
    }

    @Override public int dateEpoch() {
      return (int) DateTimeUtils.floorDiv(epoch.getMillisSinceEpoch(),
          DateTimeUtils.MILLIS_PER_DAY);
    }

    @Override public long timestampEpoch() {
      return epoch.getMillisSinceEpoch();
    }

    @Override Builder replicate(Builder b) {
      return b.addSub(name, divide, multiplier, base.name, epoch);
    }

    /** Returns a copy of this TimeFrameImpl with a given epoch. */
    Builder replicateWithEpoch(Builder b, TimestampString epoch) {
      return b.addSub(name, divide, multiplier, base.name, epoch);
    }

    @Override protected void expand(Map<TimeFrame, BigFraction> map,
        BigFraction f) {
      super.expand(map, f);
      base.expand(map, divide ? f.divide(multiplier) : f.multiply(multiplier));
    }
  }
}
