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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParserTest;

import org.junit.jupiter.api.Test;

/** Tests test fixtures.
 *
 * <p>The key feature of fixtures is that they work outside of Calcite core
 * tests, and of course this test cannot verify that. So, additional tests will
 * be needed elsewhere. The code might look similar in these additional tests,
 * but the most likely breakages will be due to classes not being on the path.
 *
 * @see Fixtures */
public class FixtureTest {
  /** Tests that you can write parser tests via {@link Fixtures#forParser()}. */
  @Test void testParserFixture() {
    // 'as' as identifier is invalid with Core parser
    final SqlParserTest.Sql f = Fixtures.forParser();
    f.sql("select ^as^ from t")
        .fails("(?s)Encountered \"as\".*");

    // Postgres cast is invalid with core parser
    f.sql("select 1 ^:^: integer as x")
        .fails("(?s).*Encountered \":\" at .*");

    // Backtick fails
    f.sql("select ^`^foo` from `bar``")
        .fails("(?s)Lexical error at line 1, column 8.  "
            + "Encountered: \"`\" \\(96\\), .*");

    // After changing config, backtick succeeds
    f.sql("select `foo` from `bar`")
        .withConfig(c -> c.withQuoting(Quoting.BACK_TICK))
        .ok("SELECT `foo`\n"
            + "FROM `bar`");
  }

  /** Tests that you can run validator tests via
   * {@link Fixtures#forValidator()}. */
  @Test void testValidatorFixture() {
    final SqlValidatorTestCase.Sql f = Fixtures.forValidator();
    f.sql("select ^1 + date '2002-03-04'^")
        .fails("(?s).*Cannot apply '\\+' to arguments of"
            + " type '<INTEGER> \\+ <DATE>'.*");

    f.sql("select 1 + 2 as three")
        .type("RecordType(INTEGER NOT NULL THREE) NOT NULL");
  }
}
