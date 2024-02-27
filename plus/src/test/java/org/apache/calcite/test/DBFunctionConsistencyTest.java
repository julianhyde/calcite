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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests that ensure that the results of SQL functions are consistent across
 * different databases.
 */
public class DBFunctionConsistencyTest {
  final Emulator emulator = Emulators.instance();

  @BeforeAll
  static void setup() {
    Emulators.instance().start();
  }

  @AfterAll
  static void teardown() {
    Emulators.instance().stop();
  }

  @ParameterizedTest
  @CsvSource({
      "SQRT(4.0),ORACLE,2",
      "SQRT(4.0),POSTGRES_9_6,2.000000000000000",
      "SQRT(4.0),POSTGRES_12_2,2.000000000000000",
      "SQRT(4.0),MYSQL,2",
      "SQRT(4),ORACLE,2",
      "SQRT(4),POSTGRES_9_6,2",
      "SQRT(4),POSTGRES_12_2,2",
      "SQRT(4),MYSQL,2",
      "SQRT(-1),ORACLE,ERROR",
      "SQRT(-1),POSTGRES_9_6,ERROR",
      "SQRT(-1),POSTGRES_12_2,ERROR",
      "SQRT(-1),MYSQL,"
  })
  void testFunction(String function, String db, String expectedResult) {
    assertThat(
        emulator.execute(Emulator.Type.valueOf(db), function),
        is(expectedResult));
  }
}
