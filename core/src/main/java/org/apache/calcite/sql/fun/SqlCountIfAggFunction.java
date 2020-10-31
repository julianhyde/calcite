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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import javax.annotation.Nonnull;

/**
 * Definition of the SQL <code>COUNTIF</code> aggregate function.
 *
 * <p><code>COUNTIF</code> returns the number of rows for which its boolean
 * expression evaluates to TRUE. {@code COUNTIF(b)} is equivalent to
 * {@code COUNT(*) FILTER (WHERE b)}.
 */
public class SqlCountIfAggFunction extends SqlAggFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCountIfAggFunction(String name) {
    super(name, null, SqlKind.COUNTIF, ReturnTypes.BIGINT, null,
        OperandTypes.BOOLEAN, SqlFunctionCategory.NUMERIC, false, false,
        Optionality.FORBIDDEN);
  }

  //~ Methods ----------------------------------------------------------------

  @Override @Nonnull public Optionality getDistinctOptionality() {
    return Optionality.FORBIDDEN;
  }
}
