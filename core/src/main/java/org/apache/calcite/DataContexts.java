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
package org.apache.calcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.Map;

/** Utilities for {@link DataContext}. */
public class DataContexts {
  private DataContexts() {
  }

  /** Instance of {@link DataContext} that has no variables. */
  public static final DataContext EMPTY = new EmptyDataContext();

  /** Returns an instance of {@link DataContext} with the given map. */
  public static DataContext of(Map<String, Object> map) {
    return new MapDataContext(map);
  }

  /** Returns an instance of {@link DataContext} with the given connection
   * and root schema but no variables. */
  public static DataContext of(CalciteConnection connection,
      @Nullable SchemaPlus rootSchema) {
    return new DataContextImpl(connection, rootSchema, ImmutableMap.of());
  }

  /** Implementation of {@link DataContext} that has no variables.
   *
   * <p>It is {@link Serializable} for Spark's benefit. */
  private static class EmptyDataContext implements DataContext, Serializable {
    @Override public @Nullable SchemaPlus getRootSchema() {
      return null;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      throw new UnsupportedOperationException();
    }

    @Override public QueryProvider getQueryProvider() {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable Object get(String name) {
      return null;
    }
  }

  /** Implementation of {@link DataContext} backed by a Map. */
  private static class MapDataContext extends EmptyDataContext {
    private final ImmutableMap<String, Object> map;

    MapDataContext(Map<String, Object> map) {
      this.map = ImmutableMap.copyOf(map);
    }

    @Override public @Nullable Object get(String name) {
      return map.get(name);
    }
  }

  /** Implementation of {@link DataContext} backed by a Map. */
  private static class DataContextImpl extends MapDataContext {
    private CalciteConnection connection;
    private SchemaPlus rootSchema;

    DataContextImpl(CalciteConnection connection,
        @Nullable SchemaPlus rootSchema, Map<String, Object> map) {
      super(map);
      this.connection = connection;
      this.rootSchema = rootSchema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return connection.getTypeFactory();
    }

    @Override public @Nullable SchemaPlus getRootSchema() {
      return rootSchema;
    }

    @Override public QueryProvider getQueryProvider() {
      return connection;
    }
  }
}
