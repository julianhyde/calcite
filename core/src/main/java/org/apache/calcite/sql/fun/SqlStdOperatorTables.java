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

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.Properties;

/**
 * Utilities for {@link SqlOperatorTable}s.
 */
public class SqlStdOperatorTables extends ReflectiveSqlOperatorTable {

  private static final Supplier<SqlOperatorTable> SPATIAL =
      Suppliers.memoize(() -> {
        if (true) {
          return CalciteCatalogReader.operatorTable(
              GeoFunctions.class.getName(),
              SqlGeoFunctions.class.getName());
        }
        final CalciteSchema root = CalciteSchema.createRootSchema(false);
        SchemaPlus rootSchema = root.plus();
        final ImmutableList<String> path = ImmutableList.of();
        ModelHandler.addFunctions(rootSchema, null, path,
            GeoFunctions.class.getName(), "*", true);
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final CalciteConnectionConfigImpl config =
            new CalciteConnectionConfigImpl(new Properties());
        return new CalciteCatalogReader(root, path, typeFactory, config);
      });

  /** Returns the Spatial operator table, creating it if necessary. */
  public static SqlOperatorTable spatialInstance() {
    return SPATIAL.get();
  }

  /** Creates a composite operator table. */
  public static SqlOperatorTable chain(Iterable<SqlOperatorTable> tables) {
    final ImmutableList<SqlOperatorTable> list =
        ImmutableList.copyOf(tables);
    if (list.size() == 1) {
      return list.get(0);
    }
    return new ChainedSqlOperatorTable(list);
  }
}

// End SqlStdOperatorTables.java
