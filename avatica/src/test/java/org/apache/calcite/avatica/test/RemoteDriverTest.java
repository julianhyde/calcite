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
package org.apache.calcite.avatica.test;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.MockJsonService;
import org.apache.calcite.avatica.remote.Service;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for Avatica Remote JDBC driver.
 */
public class RemoteDriverTest {
  public static final String MJS =
      MockJsonService.Factory.class.getName();

  public static final String LJS =
      LocalJdbcServiceFactory.class.getName();

  public static final String QRJS =
      QuasiRemoteJdbcServiceFactory.class.getName();

  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;

  private Connection mjs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + MJS);
  }

  private Connection ljs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + QRJS);
  }

  @Test public void testRegister() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:");
    assertThat(connection.isClosed(), is(false));
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testSchemas() throws Exception {
    final Connection connection = mjs();
    final ResultSet resultSet =
        connection.getMetaData().getSchemas(null, null);
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.getColumnCount() >= 2);
    assertEquals("TABLE_CATALOG", metaData.getColumnName(1));
    assertEquals("TABLE_SCHEM", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
  }

  @Test public void testTables() throws Exception {
    final Connection connection = mjs();
    final ResultSet resultSet =
        connection.getMetaData().getTables(null, null, null, new String[0]);
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.getColumnCount() >= 3);
    assertEquals("TABLE_CAT", metaData.getColumnName(1));
    assertEquals("TABLE_SCHEM", metaData.getColumnName(2));
    assertEquals("TABLE_NAME", metaData.getColumnName(3));
    resultSet.close();
    connection.close();
  }

  @Ignore
  @Test public void testNoFactory() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:");
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Ignore
  @Test public void testCatalogsMock() throws Exception {
    final Connection connection = mjs();
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testStatementExecuteQueryLocal() throws Exception {
    checkStatementExecuteQuery(ljs());
  }

  @Test public void testStatementExecuteQuery() throws Exception {
    checkStatementExecuteQuery(mjs());
  }

  private void checkStatementExecuteQuery(Connection connection)
      throws SQLException {
    final Statement statement = connection.createStatement();
    final ResultSet resultSet =
        statement.executeQuery("select * from (\n"
            + "  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)");
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("C1", metaData.getColumnName(1));
    assertEquals("C2", metaData.getColumnName(2));
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    connection.close();
  }

  /** Factory that creates a service based on a local JDBC connection. */
  public static class LocalJdbcServiceFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        Connection connection1 =
            DriverManager.getConnection(CONNECTION_SPEC.url,
                CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        return new LocalService(new JdbcMeta(connection1));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Factory that creates a service based on a local JDBC connection. */
  public static class QuasiRemoteJdbcServiceFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        Connection connection1 =
            DriverManager.getConnection(CONNECTION_SPEC.url,
                CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        final JdbcMeta jdbcMeta = new JdbcMeta(connection1);
        final LocalService localService = new LocalService(jdbcMeta);
        final LocalJsonService localJsonService =
            new LocalJsonService(localService);
        return localJsonService;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Information necessary to create a JDBC connection. Specify one to run
   * tests against a different database. (hsqldb is the default.) */
  public static class ConnectionSpec {
    public final String url;
    public final String username;
    public final String password;
    public final String driver;

    public ConnectionSpec(String url, String username, String password,
        String driver) {
      this.url = url;
      this.username = username;
      this.password = password;
      this.driver = driver;
    }

    public static final ConnectionSpec HSQLDB =
        new ConnectionSpec(
            "jdbc:hsqldb:res:foodmart", "FOODMART", "FOODMART",
            "org.hsqldb.jdbcDriver");

    public static final ConnectionSpec MYSQL =
        new ConnectionSpec(
            "jdbc:mysql://localhost/foodmart", "foodmart", "foodmart",
            "com.mysql.jdbc.Driver");
  }
}

// End RemoteDriverTest.java
