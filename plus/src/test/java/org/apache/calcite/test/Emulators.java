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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Implementations of {@link Emulator}.
 */
abstract class Emulators {
  private static final Emulator INSTANCE = new EmulatorImpl();

  /** Creates an Emulator. */
  public static Emulator instance() {
    return INSTANCE;
  }

  /** Implementation of Emulator based upon testContainers. */
  private static class EmulatorImpl implements Emulator {
    final Map<Emulator.Type, JdbcDatabaseContainer<?>> containers =
        ImmutableMap.<Emulator.Type, JdbcDatabaseContainer<?>>builder()
            .put(Type.POSTGRES_9_6,
                new PostgreSQLContainer<>("postgres:9.6"))
            .put(Type.POSTGRES_12_2,
                new PostgreSQLContainer<>("postgres:12.2"))
            .put(Type.MYSQL, new MySQLContainer<>("mysql:5.7.34"))
            .put(Type.ORACLE,
                new OracleContainer("gvenzl/oracle-xe:21-slim-faststart"))
            .build();

    /** Whether to execute a statement using a raw JDBC connection.
     * If false, execute via a Connection to Calcite with a JDBC schema. */
    final boolean raw = true;

    @Override public void start() {
      containers.values().forEach(GenericContainer::start);
    }

    @Override public void stop() {
      containers.values().forEach(GenericContainer::stop);
    }

    @Override public String execute(Emulator.Type dbType, String exp) {
      try (Connection c = getConnection(dbType)) {
        try (PreparedStatement stmt = c.prepareStatement(query(dbType, exp))) {
          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              return rs.getString(1);
            } else {
              throw new AssertionError("NoResult");
            }
          }
        }
      } catch (Exception e) {
        return "ERROR";
      }
    }

    private Connection getConnection(Emulator.Type dbType) throws SQLException {
      JdbcDatabaseContainer<?> db = containers.get(dbType);
      if (raw) {
        return DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(),
            db.getPassword());
      } else {
        String calciteUrl = "jdbc:calcite:schemaType=JDBC"
            + ";schema.jdbcUser=" + db.getUsername()
            + ";schema.jdbcPassword=" + db.getPassword()
            + ";schema.jdbcUrl=" + db.getJdbcUrl();
        return DriverManager.getConnection(calciteUrl);
      }
    }

    private String query(Emulator.Type dbType, String exp) {
      if (raw) {
        return dbType.query(exp);
      } else {
        return "SELECT " + exp;
      }
    }
  }
}
