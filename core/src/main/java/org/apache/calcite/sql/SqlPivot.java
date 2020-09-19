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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Parse tree node that represents a PIVOT clause of a SELECT statement.
 */
public class SqlPivot extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  final SqlNodeList aggList;
  final SqlNodeList axisList;
  final SqlNodeList inList;

  static final Operator OPERATOR = new Operator(SqlKind.PIVOT);

  //~ Constructors -----------------------------------------------------------

  public SqlPivot(SqlParserPos pos,
      SqlNodeList aggList,
      SqlNodeList axisList,
      SqlNodeList inList) {
    super(pos);
    this.aggList = Objects.requireNonNull(aggList);
    this.axisList = Objects.requireNonNull(axisList);
    this.inList = Objects.requireNonNull(inList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override @Nonnull public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(aggList, axisList, inList);
  }

  /** Pivot operator. */
  public static class Operator extends SqlInternalOperator {
    public Operator(SqlKind kind) {
      super(kind.name(), kind);
    }
  }
}
