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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.calcite.util.NameSet.COMPARATOR;

/** Helps construct case-insensitive ranges of {@link NameSet},
 * {@link NameMap}, {@link NameMultimap}.
 *
 * <p>Not thread-safe. */
class NameHelper {
  private final StringBuilder floor = new StringBuilder();
  private final StringBuilder ceil = new StringBuilder();

  /** Given a string, computes the smallest and largest strings that are
   * case-insensitive equal to that string,
   * calls the given function,
   * and returns its result.
   *
   * <p>For latin strings such as "bAz" computing the smallest and largest
   * strings is straightforward:
   * the floor is the upper-case string ("BAZ"), and
   * the ceil is the lower-case string ("baz").
   *
   * <p>It's more complicated for non-Latin strings that have characters
   * whose lower-case value is less than their upper-case value.
   *
   * <p>This method is not thread-safe.
   */
  private <R> R foo(String name, BiFunction<String, String, R> f) {
    for (int i = 0; i < name.length(); i++) {
      final int c = name.charAt(i);
      final int up = Character.toUpperCase(c);
      final int low = Character.toLowerCase(c);
      if (up < low) {
        floor.appendCodePoint(up);
        ceil.appendCodePoint(low);
      } else {
        floor.appendCodePoint(low);
        ceil.appendCodePoint(up);
      }
    }
    final String floorString = floor.toString();
    floor.setLength(0);
    final String ceilString = ceil.toString();
    ceil.setLength(0);
    assert floorString.compareTo(ceilString) <= 0;
    return f.apply(floorString, ceilString);
  }

  /** Used by {@link NameSet#range(String, boolean)}. */
  Collection<String> set(NavigableSet<String> names, String name) {
    return foo(name,
        (floor, ceil) -> {
          final NavigableSet<String> subSet =
              names.subSet(floor, true, ceil, true);
          return subSet
              .stream()
              .filter(s -> s.equalsIgnoreCase(name))
              .collect(Collectors.toList());
        });
  }

  /** Used by {@link NameMap#range(String, boolean)}. */
  <V> ImmutableSortedMap<String, V> map(NavigableMap<String, V> map,
      String name) {
    return foo(name,
        (floor, ceil) -> {
          final ImmutableSortedMap.Builder<String, V> builder =
              new ImmutableSortedMap.Builder<>(COMPARATOR);
          final NavigableMap<String, V> subMap =
              map.subMap(floor, true, ceil, true);
          for (Map.Entry<String, V> e : subMap.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
              builder.put(e.getKey(), e.getValue());
            }
          }
          return builder.build();
        });
  }

  /** Used by {@link NameMultimap#range(String, boolean)}. */
  <V> Collection<Map.Entry<String, V>> multimap(
      NavigableMap<String, List<V>> map, String name) {
    return foo(name,
        (floor, ceil) -> {
          final NavigableMap<String, List<V>> subMap =
              map.subMap(floor, true, ceil, true);
          final ImmutableList.Builder<Map.Entry<String, V>> builder =
              ImmutableList.builder();
          for (Map.Entry<String, List<V>> e : subMap.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
              for (V v : e.getValue()) {
                builder.add(Pair.of(e.getKey(), v));
              }
            }
          }
          return builder.build();
        });
  }
}

// End NameHelper.java
