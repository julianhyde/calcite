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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
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

    /** Converts a number to an exactly equivalent {@code BigFraction}.
     * May silently lose precision if n is a {@code Float} or {@code Double}. */
    static BigFraction toFraction(Number n) {
      if (n instanceof BigFraction) {
        return (BigFraction) n;
      } else if (n instanceof BigInteger) {
        return new BigFraction((BigInteger) n);
      } else if (n instanceof BigDecimal) {
        BigDecimal bd = (BigDecimal) n;
        return new BigFraction(bd.unscaledValue())
            .multiply(BigInteger.TEN.pow(-bd.scale()));
      } else {
        return new BigFraction(n.longValue());
      }
    }

    public Builder addCore(String name) {
      map.put(name,
          new TimeFrameImpl(name, null, TimestampString.EPOCH));
      return this;
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    public Builder addMultiple(String name, Number count,
        String baseName) {
      final TimeFrameImpl baseFrame = map.get(baseName);
      map.put(name,
          new TimeFrameImpl(name,
              Pair.of(baseFrame, toFraction(count)),
              TimestampString.EPOCH));
      return this;
    }

    /** Defines such that each {@code baseUnit} consists of {@code count}
     * instances of the new unit. */
    public Builder addDivision(String name, Number count, String baseName) {
      BigFraction f = toFraction(count);
      return addMultiple(name, BigFraction.ONE.divide(f), baseName);
    }

    /** Adds all time frames in {@code timeFrameSet} to this Builder. */
    public Builder addAll(TimeFrameSet timeFrameSet) {
      timeFrameSet.map.forEach((k, v) -> map.put(k, (TimeFrameImpl) v));
      return this;
    }

    /** Replaces the epoch of the most recently added frame. */
    public void withEpoch(TimestampString epoch) {
      Map.Entry<String, TimeFrameImpl> lastEntry =
          Iterables.getLast(map.entrySet());
      lastEntry.setValue(lastEntry.getValue().withEpoch(epoch));
    }
  }

  /** Implementation of {@link TimeFrame}. */
  static class TimeFrameImpl implements TimeFrame {
    private final String name;
    private final @Nullable Pair<TimeFrameImpl, BigFraction> composedOf;
    private final TimestampString epoch;

    TimeFrameImpl(String name,
        @Nullable Pair<TimeFrameImpl, BigFraction> composedOf,
        TimestampString epoch) {
      this.name = requireNonNull(name, "name");
      this.composedOf = composedOf;
      this.epoch = epoch;
    }

    @Override public String toString() {
      final StringBuilder b = new StringBuilder();
      b.append(name);
      if (composedOf != null) {
        b.append(", composedOf ").append(composedOf.right)
            .append(" ").append(composedOf.left);
      }
      return b.toString();
    }

    @Override public String name() {
      return name;
    }

    @Override public @Nullable Pair<TimeFrameImpl, BigFraction> composedOf() {
      return composedOf;
    }

    @Override public Collection<TimeFrame> alignsWith() {
      return null;
    }

    @Override public Map<TimeFrame, Pair<Number, TimestampString>> zero() {
      return ImmutableMap.of();
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

    private void expand(Map<TimeFrame, BigFraction> map, BigFraction f) {
      map.put(this, f);
      if (composedOf != null && composedOf.right != null) {
        composedOf.left.expand(map, composedOf.right.multiply(f));
      }
    }

    @Override public int dateEpoch() {
      return (int) DateTimeUtils.floorDiv(epoch.getMillisSinceEpoch(),
          DateTimeUtils.MILLIS_PER_DAY);
    }

    @Override public long timestampEpoch() {
      return epoch.getMillisSinceEpoch();
    }

    /** Returns a copy of this TimeFrameImpl with a given epoch. */
    TimeFrameImpl withEpoch(TimestampString epoch) {
      return this.epoch.equals(epoch) ? this
          : new TimeFrameImpl(name, composedOf, epoch);
    }
  }

}
