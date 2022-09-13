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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Map;

/** Time frame.
 *
 */
public interface TimeFrame {
  /** Name of this time frame.
   *
   * <p>A time unit based on a built-in Avatica
   * {@link org.apache.calcite.avatica.util.TimeUnit} will have the same
   * name. */
  String name();

  /** Time frame that this time unit is composed of, and the multiple.
   *
   * <p>The multiple should not be floating point; use an exact type such as
   * {@link Integer}, {@link java.math.BigDecimal} or {@link BigFraction}.
   *
   * <p>For example,
   * {@code MINUTE.composedOf()} returns {@code Pair(SECOND, 60)};
   * {@code MILLISECOND.composedOf()} returns
   * {@code Pair(SECOND, BigFraction(1, 1000))}.
   *
   * <p>If {@code T1.isComposedOf()} returns {@code Pair(T2, N} and {@code N} is
   * a positive integer, this implies that {@code T2} is aligned with
   * {@code T1}, that is, every instance of {@code T2} belongs to one instance
   * of {@code T1}.
   */
  @Nullable Pair<? extends TimeFrame, ? extends Number> composedOf();

  /** A collection of time frames that this time frame aligns with.
   *
   * <p>For example:
   * DAY aligns with WEEK, MONTH, YEAR (because each day belongs to
   * exactly one week, month and year);
   * WEEK does not align with MONTH or YEAR (because a particular week can cross
   * month or year boundaries);
   * MONTH aligns with YEAR.
   * Therefore, DAY declares that it aligns with WEEK, MONTH,
   * and DAY's alignment with YEAR can be inferred from MONTH's alignment with
   * YEAR.
   */
  Collection<TimeFrame> alignsWith();

  /** A point in time when this time frame causes its parent to advance.
   *
   * <p>For example, the map returned by DAY.zero() might contain the entry
   * (WEEK, Timestamp(1900, 1, 1)), because 1900/01/01 was a Monday, the first
   * day of the week. Because we know that a week is 7 days, we can compute
   * every other point at which a week advances.
   *
   * <p>The map returned from {@code DAY.zero()} does not contain an entry for
   * {@code MONTH} because the mapping is so complex that custom logic is
   * required. Likewise for rolling from {@code ISOWEEK} to {@code ISOYEAR}. */
  Map<TimeFrame, Pair<Number, TimestampString>> zero();

  /** If this time frame has units in common with another time frame, returns
   * the number of this time frame in one of that time frame.
   *
   * <p>For example, MONTH.per(YEAR) returns 12; YEAR.per(MONTH) returns 1 / 12.
   */
  @Nullable Number per(TimeFrame timeFrame);
}
