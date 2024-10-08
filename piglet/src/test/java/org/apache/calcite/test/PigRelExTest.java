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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.TestUtil;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.calcite.test.Matchers.inTree;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/**
 * Tests for {@code PigRelExVisitor}.
 */
class PigRelExTest extends PigRelTestBase {
  private void checkTranslation(String pigExpr, Matcher<RelNode> relMatcher) {
    String pigScript = ""
        + "A = LOAD 'test' as (a:int, b:long, c:float, d:double,\n"
        + "    e:chararray, f:bytearray, g:boolean, h:datetime,\n"
        + "    i:biginteger, j:bigdecimal, k1:tuple(),\n"
        + "    k2:tuple(k21:int, k22:(k221:long, k222:chararray)), l1:bag{},\n"
        + "    l2:bag{(l21:int, l22:float)}, m1:map[], m2:map[int],\n"
        + "    m3:map[(m31:float)]);\n"
        + "B = FILTER A BY " + pigExpr + ";\n";
    try {
      final RelNode rel =
          converter.pigQuery2Rel(pigScript, false, false, false).get(0);
      assertThat(rel, relMatcher);
    } catch (IOException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private void checkType(String pigExpr, Matcher<String> rowTypeMatcher) {
    String pigScript = ""
        + "A = LOAD 'test' as (a:int);\n"
        + "B = FOREACH A GENERATE a, " + pigExpr + ";\n";
    try {
      final RelNode rel =
          converter.pigQuery2Rel(pigScript, false, false, false).get(0);
      assertThat(rel.getRowType(), hasToString(rowTypeMatcher));
    } catch (IOException e) {
      throw TestUtil.rethrow(e);
    }
  }

  @Test void testConstantBoolean() {
    checkTranslation("g == false", inTree("NOT($6)"));
  }

  @Test void testConstantType() {
    checkType("0L as longCol", containsString("BIGINT longCol"));
    checkType("0 as intCol", containsString("INTEGER intCol"));
    checkType("0.0 as doubleCol", containsString("DOUBLE doubleCol"));
    checkType("'0.0' as charCol", containsString("CHAR(3) charCol"));
    checkType("true as boolCol", containsString("BOOLEAN boolCol"));
  }

  @Test void testConstantFloat() {
    // Add a variable d in the expression to prevent it from being simplified to "false".
    checkTranslation(".1E6 == -2.3 + d",
        // Validator converts -2.3 from DECIMAL to DOUBLE
        inTree("=(100000.0E0, +(-2.3E0, $3))"));
  }

  @Test void testConstantString() {
    checkTranslation("'test' == 'passed'", inTree("=('test', 'passed')"));
  }

  @Test void testProjection() {
    checkTranslation("g", inTree("=[$6]"));
  }

  @Test void testNegation() {
    checkTranslation("-b == -6", inTree("=(-($1), -6)"));
  }

  @Test void testEqual() {
    checkTranslation("a == 10", inTree("=($0, 10)"));
  }

  @Test void testNotEqual() {
    checkTranslation("b != 10", inTree("<>($1, 10)"));
  }

  @Test void testLessThan() {
    checkTranslation("b < 10", inTree("<($1, 10)"));
  }

  @Test void testLessThanEqual() {
    checkTranslation("b <= 10", inTree("<=($1, 10)"));
  }

  @Test void testGreaterThan() {
    checkTranslation("b > 10", inTree(">($1, 10)"));
  }

  @Test void testGreaterThanEqual() {
    checkTranslation("b >= 10", inTree(">=($1, 10)"));
  }

  @Test @Disabled
  public void testMatch() {
    checkTranslation("e matches 'A*BC.D'", inTree("LIKE($4, 'A%BC_D')"));
  }

  @Test void testIsNull() {
    checkTranslation("e is null", inTree("IS NULL($4)"));
  }

  @Test void testIsNotNull() {
    checkTranslation("c is not null", inTree("IS NOT NULL($2)"));
  }

  @Test void testNot() {
    checkTranslation("NOT(a is null)", inTree("IS NOT NULL($0)"));
    checkTranslation("NOT(g)", inTree("NOT($6)"));
  }

  @Test void testAnd() {
    checkTranslation("a > 10 and g", inTree("AND(>($0, 10), $6)"));
  }

  @Test void testOr() {
    checkTranslation("a > 10 or g", inTree("OR(>($0, 10), $6)"));
  }

  @Test void testAdd() {
    checkTranslation("b + 3", inTree("+($1, 3)"));
  }

  @Test void testSubtract() {
    checkTranslation("b - 3", inTree("-($1, 3)"));
  }

  @Test void testMultiply() {
    checkTranslation("b * 3", inTree("*($1, 3)"));
  }

  @Test void testMod() {
    checkTranslation("b % 3", inTree("MOD($1, 3)"));
  }

  @Test void testDivide() {
    checkTranslation("b / 3", inTree("/($1, 3)"));
    checkTranslation("c / 3.1", inTree("/($2, 3.1E0:DOUBLE)"));
  }

  @Test void testBinCond() {
    checkTranslation("(b == 1 ? 2 : 3)", inTree("CASE(=($1, 1), 2, 3)"));
  }

  @Test void testTupleDereference() {
    checkTranslation("k2.k21", inTree("[$11.k21]"));
    checkTranslation("k2.(k21, k22)", inTree("[ROW($11.k21, $11.k22)]"));
    checkTranslation("k2.k22.(k221,k222)",
        inTree("[ROW($11.k22.k221, $11.k22.k222)]"));
  }

  @Test void testBagDereference() {
    checkTranslation("l2.l22", inTree("[MULTISET_PROJECTION($13, 1)]"));
    checkTranslation("l2.(l21, l22)", inTree("[MULTISET_PROJECTION($13, 0, 1)]"));
  }

  @Test void testMapLookup() {
    checkTranslation("m2#'testKey'", inTree("ITEM($15, 'testKey')"));
  }

  @Test void testCast() {
    checkTranslation("(int) b", inTree("CAST($1):INTEGER"));
    checkTranslation("(long) a", inTree("CAST($0):BIGINT"));
    checkTranslation("(float) b", inTree("CAST($1):REAL"));
    checkTranslation("(double) b", inTree("CAST($1):DOUBLE"));
    checkTranslation("(chararray) b", inTree("CAST($1):VARCHAR"));
    checkTranslation("(bytearray) b", inTree("CAST($1):BINARY"));
    checkTranslation("(boolean) c", inTree("CAST($2):BOOLEAN"));
    checkTranslation("(biginteger) b", inTree("CAST($1):DECIMAL(19, 0)"));
    checkTranslation("(bigdecimal) b", inTree("CAST($1):DECIMAL(19, 0)"));
    checkTranslation("(tuple()) b", inTree("CAST($1):(DynamicRecordRow[])"));
    checkTranslation("(tuple(int, float)) b",
        inTree("CAST($1):RecordType(INTEGER $0, REAL $1)"));
    checkTranslation("(bag{}) b",
        inTree("CAST($1):(DynamicRecordRow[]) NOT NULL MULTISET"));
    checkTranslation("(bag{tuple(int)}) b",
        inTree("CAST($1):RecordType(INTEGER $0) MULTISET"));
    checkTranslation("(bag{tuple(int, float)}) b",
        inTree("CAST($1):RecordType(INTEGER $0, REAL $1) MULTISET"));
    checkTranslation("(map[]) b",
        inTree("CAST($1):(VARCHAR NOT NULL, BINARY(1) NOT NULL) MAP"));
    checkTranslation("(map[int]) b", inTree("CAST($1):(VARCHAR NOT NULL, INTEGER"));
    checkTranslation("(map[tuple(int, float)]) b",
        inTree("CAST($1):(VARCHAR NOT NULL, RecordType(INTEGER val_0, REAL val_1)) MAP"));
  }

  @Test void testPigBuiltinFunctions() {
    checkTranslation("ABS(-5)", inTree("ABS(-5)"));
    checkTranslation("AddDuration(h, 'P1D')",
        inTree("AddDuration(PIG_TUPLE($7, 'P1D'))"));
    checkTranslation("CEIL(1.2)", inTree("CEIL(1.2E0:DOUBLE)"));
  }
}
