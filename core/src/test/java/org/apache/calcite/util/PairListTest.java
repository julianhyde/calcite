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

import org.apache.calcite.runtime.PairList;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Unit test for {@code PairList}. */
class PairListTest {
  /** Basic test for {@link PairList}. */
  @Test void testPairList() {
    final PairList<Integer, String> pairList = PairList.of();
    final List<Pair<Integer, String>> list = new ArrayList<>();

    final Runnable validator = () -> {
      assertThat(pairList.isEmpty(), is(list.isEmpty()));
      assertThat(pairList.size(), is(list.size()));
      assertThat(pairList.leftList().size(), is(list.size()));
      assertThat(pairList.rightList().size(), is(list.size()));
      assertThat(pairList.leftList(), is(Pair.left(list)));
      assertThat(pairList.rightList(), is(Pair.right(list)));

      final List<Map.Entry<Integer, String>> list2 = new ArrayList<>(pairList);
      assertThat(list2, is(list));

      // Check PairList.forEach(Consumer)
      list2.clear();
      //noinspection UseBulkOperation
      pairList.forEach(p -> list2.add(p));
      assertThat(list2, is(list));

      // Check PairList.forEach(BiConsumer)
      list2.clear();
      pairList.forEach((k, v) -> list2.add(Pair.of(k, v)));
      assertThat(list2, is(list));
    };

    validator.run();

    pairList.add(1, "a");
    list.add(Pair.of(1, "a"));
    validator.run();

    pairList.add(Pair.of(2, "b"));
    list.add(Pair.of(2, "b"));
    validator.run();

    pairList.add(0, Pair.of(3, "c"));
    list.add(0, Pair.of(3, "c"));
    validator.run();

    Map.Entry<Integer, String> x = pairList.remove(1);
    Pair<Integer, String> y = list.remove(1);
    assertThat(x, is(y));
    validator.run();

    pairList.clear();
    list.clear();
    validator.run();
  }
}
