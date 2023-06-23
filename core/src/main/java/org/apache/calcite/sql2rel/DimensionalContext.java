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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static java.util.Objects.requireNonNull;

/** Describes the list of dimensions available to an expression.
 *
 * <p>Consider the expression
 *
 * <blockquote><pre>{@code
 * avg_sal AT (CLEAR deptno
 *             DEFINE year = YEAR(hiredate)
 *             SET year = CURRENT year - 1)
 * }</pre></blockquote>
 *
 * <p>Steps as follows:
 *
 * <ul>
 * <li>Initially the dimensionality is [deptno, job, hiredate].
 * <li>After {@code CLEAR}, the dimensionality is the same
 *   (but we are no longer filtering on {@code deptno}).
 * <li>After {@code DEFINE}, the dimensionality is
 *   [deptno, job, hiredate, year].
 * <li>After {@code SET}, the value of {@code year} has changed.
 * </ul>
 */
class DimensionalContext {
  final RelNode root;
  final SqlValidatorScope scope;

  DimensionalContext(RelNode root, SqlValidatorScope scope) {
    this.root = requireNonNull(root, "root");
    this.scope = scope;
  }
}
