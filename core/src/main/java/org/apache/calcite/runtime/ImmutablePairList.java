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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;

/** Immutable list of pairs.
 *
 * @param <T> First type
 * @param <U> Second type
 */
public class ImmutablePairList<T, U> extends PairList<T, U> {
  private static final ImmutablePairList<Object, Object> EMPTY =
      new ImmutablePairList<>(ImmutableList.of());

  @SuppressWarnings({"unchecked", "rawtypes"})
  ImmutablePairList(ImmutableList<Object> list) {
    super((ImmutableList) list);
  }

  /** Creates an empty ImmutablePairList. */
  @SuppressWarnings("unchecked")
  public static <T, U> ImmutablePairList<T, U> of() {
    return (ImmutablePairList<T, U>) EMPTY;
  }

  /** Creates a singleton ImmutablePairList. */
  public static <T, U> ImmutablePairList<T, U> of(@NonNull T t, @NonNull U u) {
    return new ImmutablePairList<>(ImmutableList.of(t, u));
  }

  @SuppressWarnings("unchecked")
  public static <T, U> ImmutablePairList<T, U> copyOf(
      Iterable<? extends Map.Entry<@NonNull T, @NonNull U>> iterable) {
    if (iterable instanceof PairList) {
      return ((PairList<T, U>) iterable).immutable();
    }
    ImmutableList.Builder<Object> builder = ImmutableList.builder();
    iterable.forEach(p -> {
      builder.add(p.getKey());
      builder.add(p.getValue());
    });
    return new ImmutablePairList<>(builder.build());
  }

  @Override public ImmutablePairList<T, U> immutable() {
    return this;
  }

  @Override public ImmutablePairList<T, U> subList(int fromIndex, int toIndex) {
    return new ImmutablePairList<>(
        ((ImmutableList<Object>) list).subList(fromIndex * 2, toIndex * 2));
  }

  @Deprecated // will always throw
  @Override public void reverse() {
    throw new UnsupportedOperationException();
  }
}
