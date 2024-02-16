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
package org.apache.calcite.sql.validate;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Extension to {@link SqlValidatorTable} with extra, optional metadata.
 *
 * <p>Used to flag individual columns as 'must-filter'.
 */
public interface SemanticTable {
  /** Returns the filter expression for column {@code columnName}
   * if it is a must-filter column,
   * or null if it is not a must-filter column.
   *
   * @see #mustFilter(String) */
  @Nullable String getFilter(String columnName);

  /** Returns whether column {@code columnName} must be filtered in any query
   * that references this table. */
  boolean mustFilter(String columnName);
}
