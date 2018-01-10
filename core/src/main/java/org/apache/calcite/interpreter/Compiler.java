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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Context while converting a tree of {@link RelNode} to a program
 * that can be run by an {@link Interpreter}.
 */
public interface Compiler {
  RelDataType combinedRowType(List<RelNode> inputs);

  DataContext getDataContext();

  Context createContext();

  Source source(RelNode rel, int ordinal);

  Sink sink(RelNode rel);

  void enumerable(RelNode rel, Enumerable<Row> rowEnumerable);

  Scalar compile(List<RexNode> nodes, RelDataType inputRowType);
}

// End Compiler.java
