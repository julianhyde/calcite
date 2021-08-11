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
package org.apache.calcite.rex;

import org.apache.calcite.sql.SqlKind;

import com.google.common.collect.Sets;

import java.util.EnumSet;
import java.util.Set;

/**
 * Decides whether it is safe to flatten the given CASE part into ANDs/ORs.
 */
enum SafeRexVisitor implements RexVisitor<Boolean> {
  INSTANCE;

  @SuppressWarnings("ImmutableEnumChecker")
  private final Set<SqlKind> safeOps;

  SafeRexVisitor() {
    Set<SqlKind> safeOps = EnumSet.noneOf(SqlKind.class);

    safeOps.addAll(SqlKind.COMPARISON);
    safeOps.add(SqlKind.PLUS_PREFIX);
    safeOps.add(SqlKind.MINUS_PREFIX);
    safeOps.add(SqlKind.PLUS);
    safeOps.add(SqlKind.MINUS);
    safeOps.add(SqlKind.TIMES);
    safeOps.add(SqlKind.IS_FALSE);
    safeOps.add(SqlKind.IS_NOT_FALSE);
    safeOps.add(SqlKind.IS_TRUE);
    safeOps.add(SqlKind.IS_NOT_TRUE);
    safeOps.add(SqlKind.IS_NULL);
    safeOps.add(SqlKind.IS_NOT_NULL);
    safeOps.add(SqlKind.IS_DISTINCT_FROM);
    safeOps.add(SqlKind.IS_NOT_DISTINCT_FROM);
    safeOps.add(SqlKind.IN);
    safeOps.add(SqlKind.SEARCH);
    safeOps.add(SqlKind.OR);
    safeOps.add(SqlKind.AND);
    safeOps.add(SqlKind.NOT);
    safeOps.add(SqlKind.CASE);
    safeOps.add(SqlKind.LIKE);
    safeOps.add(SqlKind.COALESCE);
    safeOps.add(SqlKind.TRIM);
    safeOps.add(SqlKind.LTRIM);
    safeOps.add(SqlKind.RTRIM);
    safeOps.add(SqlKind.BETWEEN);
    safeOps.add(SqlKind.CEIL);
    safeOps.add(SqlKind.FLOOR);
    safeOps.add(SqlKind.REVERSE);
    safeOps.add(SqlKind.TIMESTAMP_ADD);
    safeOps.add(SqlKind.TIMESTAMP_DIFF);
    this.safeOps = Sets.immutableEnumSet(safeOps);
  }

  @Override public Boolean visitInputRef(RexInputRef inputRef) {
    return true;
  }

  @Override public Boolean visitLocalRef(RexLocalRef localRef) {
    return false;
  }

  @Override public Boolean visitLiteral(RexLiteral literal) {
    return true;
  }

  @Override public Boolean visitCall(RexCall call) {
    if (!safeOps.contains(call.getKind())) {
      return false;
    }
    return RexVisitorImpl.visitArrayAnd(this, call.operands);
  }

  @Override public Boolean visitOver(RexOver over) {
    return false;
  }

  @Override public Boolean visitCorrelVariable(
      RexCorrelVariable correlVariable) {
    return false;
  }

  @Override public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
    return false;
  }

  @Override public Boolean visitRangeRef(RexRangeRef rangeRef) {
    return false;
  }

  @Override public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
    return true;
  }

  @Override public Boolean visitSubQuery(RexSubQuery subQuery) {
    return false;
  }

  @Override public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
    return false;
  }

  @Override public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return false;
  }
}
