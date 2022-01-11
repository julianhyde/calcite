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
package org.apache.calcite.test;

import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.List;
import java.util.function.Function;

/**
 * Fluent class that contains information necessary to run a test.
 */
@Value.Immutable(singleton = false, builder = true)
public interface MaterializedViewFixture {
  static MaterializedViewFixture create(String query,
      AbstractMaterializedViewTest tester) {
    return ImmutableMaterializedViewFixture.of(query, tester);
  }

  default void ok() {
    getTester().checkMaterialize(this);
  }

  default void noMat() {
    getTester().checkNoMaterialize(this);
  }

  CalciteAssert.@Nullable SchemaSpec getDefaultSchemaSpec();

  MaterializedViewFixture withDefaultSchemaSpec(
      CalciteAssert.@Nullable SchemaSpec spec);

  List<Pair<String, String>> getMaterializations();

  MaterializedViewFixture withMaterializations(
      Iterable<? extends Pair<String, String>> materialize);

  @Value.Parameter
  String getQuery();

  MaterializedViewFixture withQuery(String query);

  @Nullable Function<String, Boolean> getChecker();

  MaterializedViewFixture withChecker(
      @Nullable Function<String, Boolean> checker);

  @Value.Parameter
  AbstractMaterializedViewTest getTester();

  MaterializedViewFixture withTester(AbstractMaterializedViewTest tester);
}
