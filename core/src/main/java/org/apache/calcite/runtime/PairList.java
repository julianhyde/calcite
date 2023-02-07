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

import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A list of pairs, stored as a quotient list. */
public class PairList<T, U> extends AbstractList<Map.Entry<T, U>> {
  final List<Object> list;

  private PairList(List<Object> list) {
    this.list = list;
  }

  /** Creates an empty PairList. */
  public static <T, U> PairList<T, U> of() {
    return new PairList<>(new ArrayList<>());
  }

  /** Creates a PairList from a Map. */
  public static <T, U> PairList<T, U> of(Map<T, U> map) {
    final List<Object> list = new ArrayList<>(map.size() * 2);
    map.forEach((t, u) -> list.add(Pair.of(t, u)));
    return new PairList<>(list);
  }

  @Override public Map.Entry<T, U> get(int index) {
    int x = index * 2;
    return Pair.of((T) list.get(x), (U) list.get(x + 1));
  }

  @Override public int size() {
    return list.size() / 2;
  }

  @Override public boolean add(Map.Entry<T, U> tuEntry) {
    list.add(tuEntry.getKey());
    return list.add(tuEntry.getValue());
  }

  @SuppressWarnings("unchecked")
  public List<T> leftList() {
    return Util.quotientList((List<T>) list, 2, 0);
  }

  @SuppressWarnings("unchecked")
  public List<U> rightList() {
    return Util.quotientList((List<U>) list, 2, 1);
  }
}
