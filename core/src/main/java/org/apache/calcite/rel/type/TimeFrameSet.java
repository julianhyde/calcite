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

import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
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

  /** Builds a collection of time frames. */
  public static class Builder {
    Builder() {
    }

    final Map<String, TimeFrameImpl> map = new HashMap<>();

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

    public void addCore(String name) {
      map.put(name,
          new TimeFrameImpl(name, null));
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    public Builder addMultiple(String name, Number count,
        String baseName) {
      final TimeFrameImpl baseFrame = map.get(baseName);
      map.put(name,
          new TimeFrameImpl(name,
              Pair.of(baseFrame, toFraction(count))));
      return this;
    }

    /** Defines such that each {@code baseUnit} consists of {@code count}
     * instances of the new unit. */
    public Builder addDivision(String name, int count, String baseName) {
      return addMultiple(name, BigFraction.ONE.divide(count), baseName);
    }

    /** Adds all time frames in {@code timeFrameSet} to this Builder. */
    public Builder addAll(TimeFrameSet timeFrameSet) {
      timeFrameSet.map.forEach((k, v) -> map.put(k, (TimeFrameImpl) v));
      return this;
    }
  }

  /** Implementation of {@link TimeFrame}. */
  static class TimeFrameImpl implements TimeFrame {
    private final String name;
    private final @Nullable Pair<TimeFrameImpl, BigFraction> composedOf;

    TimeFrameImpl(String name,
        @Nullable Pair<TimeFrameImpl, BigFraction> composedOf) {
      this.name = requireNonNull(name, "name");
      this.composedOf = composedOf;
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

    @Override public @Nullable Number per(TimeFrame timeFrame) {
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
  }

}
