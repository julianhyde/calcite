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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Set of {@link TimeFrame} definitions. */
public class TimeFrameSet {
  private final ImmutableMap<String, TimeFrameImpl> map;
  private final ImmutableMultimap<TimeFrameImpl, TimeFrameImpl> rollupMap;

  private TimeFrameSet(ImmutableMap<String, TimeFrameImpl> map,
      ImmutableMultimap<TimeFrameImpl, TimeFrameImpl> rollupMap) {
    this.map = requireNonNull(map, "map");
    this.rollupMap = requireNonNull(rollupMap, "rollupMap");
    map.values().forEach(k -> k.set = this);
  }

  /** Creates a Builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the time frame with the given name,
   * or throws {@link IllegalArgumentException}. */
  public TimeFrame get(String name) {
    final TimeFrame timeFrame = map.get(name);
    if (timeFrame == null) {
      throw new IllegalArgumentException("unknown frame: " + name);
    }
    return timeFrame;
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
    final ImmutableMultimap.Builder<TimeFrameImpl, TimeFrameImpl> rollupList =
        ImmutableMultimap.builder();

    public TimeFrameSet build() {
      return new TimeFrameSet(ImmutableMap.copyOf(map), rollupList.build());
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
      final TimeFrameImpl baseFrame = get(baseName);
      final BigInteger factor = toBigInteger(count);

      final CoreTimeFrame coreFrame = baseFrame.core();
      final BigFraction coreFactor = divide
          ? baseFrame.coreMultiplier().divide(factor)
          : baseFrame.coreMultiplier().multiply(factor);

      map.put(name,
          new SubTimeFrame(name, baseFrame, divide, factor, coreFrame,
              coreFactor, epoch));
      return this;
    }

    /** Returns the time frame with the given name,
     * or throws {@link IllegalArgumentException}. */
    TimeFrameImpl get(String name) {
      final TimeFrameImpl timeFrame = map.get(name);
      if (timeFrame == null) {
        throw new IllegalArgumentException("unknown frame: " + name);
      }
      return timeFrame;
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

    /** Defines a rollup from one frame to another.
     *
     * <p>An explicit rollup is not necessary for frames where one is a multiple
     * of another (such as MILLISECOND to HOUR). Only use this method for frames
     * that are not multiples (such as DAY to MONTH). */
    public Builder addRollup(String fromName, String toName) {
      final TimeFrameImpl fromFrame = get(fromName);
      final TimeFrameImpl toFrame = get(toName);
      rollupList.put(fromFrame, toFrame);
      return this;
    }

    /** Adds all time frames in {@code timeFrameSet} to this Builder. */
    public Builder addAll(TimeFrameSet timeFrameSet) {
      timeFrameSet.map.values().forEach(frame -> frame.replicate(this));
      return this;
    }

    /** Replaces the epoch of the most recently added frame. */
    public Builder withEpoch(TimestampString epoch) {
      final String name = Iterables.getLast(map.keySet());
      final SubTimeFrame value =
          requireNonNull((SubTimeFrame) map.remove(name));
      value.replicateWithEpoch(this, epoch);
      return this;
    }
  }

  /** Implementation of {@link TimeFrame}. */
  abstract static class TimeFrameImpl implements TimeFrame {
    final String name;

    /** Mutable, set on build, and then not re-assigned. Ideally this would be
     * final. */
    private TimeFrameSet set;

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
    abstract void replicate(Builder b);

    protected abstract CoreTimeFrame core();

    protected abstract BigFraction coreMultiplier();

    @Override public boolean canRollUpTo(TimeFrame toFrame) {
      if (toFrame == this) {
        return true;
      }
      if (toFrame instanceof TimeFrameImpl) {
        final TimeFrameImpl toFrame1 = (TimeFrameImpl) toFrame;
        if (canDirectlyRollUp(this, toFrame1)) {
          return true;
        }
        if (set.rollupMap.entries().contains(Pair.of(this, toFrame1))) {
          return true;
        }
        // Hard-code roll-up via DAY-to-MONTH bridge, for now.
        if (canDirectlyRollUp(this, (TimeFrameImpl) set.get(TimeUnit.DAY))
            && canDirectlyRollUp((TimeFrameImpl) set.get(TimeUnit.MONTH), toFrame1)) {
          return true;
        }
      }
      return false;
    }

    private static boolean canDirectlyRollUp(TimeFrameImpl from, TimeFrameImpl to) {
      if (from.core().equals(to.core())) {
        return from.coreMultiplier()
            .divide(to.coreMultiplier())
            .getNumerator()
            .equals(BigInteger.ONE);
      }
      return false;
    }
  }

  /** Core time frame (such as SECOND and MONTH). */
  static class CoreTimeFrame extends TimeFrameImpl {
    CoreTimeFrame(String name) {
      super(name);
    }

    @Override void replicate(Builder b) {
      b.addCore(name);
    }

    @Override protected CoreTimeFrame core() {
      return this;
    }

    @Override protected BigFraction coreMultiplier() {
      return BigFraction.ONE;
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
    private final CoreTimeFrame coreFrame;

    /** The number of core frames that are equivalent to one of these. For
     * example, MINUTE, HOUR, MILLISECOND all have core = SECOND, and have
     * multipliers 60, 3,600, 1 / 1,000 respectively. */
    private final BigFraction coreMultiplier;
    private final TimestampString epoch;

    SubTimeFrame(String name, TimeFrameImpl base, boolean divide,
        BigInteger multiplier, CoreTimeFrame coreFrame,
        BigFraction coreMultiplier, TimestampString epoch) {
      super(name);
      this.base = requireNonNull(base, "base");
      this.divide = divide;
      this.multiplier = requireNonNull(multiplier, "multiplier");
      this.coreFrame = requireNonNull(coreFrame, "coreFrame");
      this.coreMultiplier = requireNonNull(coreMultiplier, "coreMultiplier");
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

    @Override void replicate(Builder b) {
      b.addSub(name, divide, multiplier, base.name, epoch);
    }

    /** Returns a copy of this TimeFrameImpl with a given epoch. */
    void replicateWithEpoch(Builder b, TimestampString epoch) {
      b.addSub(name, divide, multiplier, base.name, epoch);
    }

    @Override protected void expand(Map<TimeFrame, BigFraction> map,
        BigFraction f) {
      super.expand(map, f);
      base.expand(map, divide ? f.divide(multiplier) : f.multiply(multiplier));
    }

    @Override protected CoreTimeFrame core() {
      return coreFrame;
    }

    @Override protected BigFraction coreMultiplier() {
      return coreMultiplier;
    }
  }
}
