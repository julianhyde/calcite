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
package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaFactory;

import org.olap4j.OlapConnection;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Implementation of olap4j connection
 * in the Calcite engine.
 *
 * <p>Abstract to allow newer versions of olap4j to add methods;
 * public to allow sub-projects to refine.
 */
@SuppressWarnings("WeakerAccess")
public abstract class CalciteOlapConnection extends CalciteConnectionImpl
    implements OlapConnection {
  /**
   * Creates a CalciteOlapConnection.
   *
   * <p>Not public; method is called only from the driver
   * or from a sub-class.
   *
   * @param driver Driver
   * @param factory Factory for JDBC objects
   * @param url Server URL
   * @param info Other connection properties
   * @param rootSchema Root schema, or null
   * @param typeFactory Type factory, or null
   */
  protected CalciteOlapConnection(Driver driver, AvaticaFactory factory,
      String url, Properties info, CalciteSchema rootSchema,
      JavaTypeFactory typeFactory) {
    super(driver, factory, url, info, rootSchema, typeFactory);
  }

  @Override public OlapDatabaseMetaData getMetaData() throws OlapException {
    try {
      return (OlapDatabaseMetaData) super.getMetaData();
    } catch (SQLException e) {
      throw toOlap(e);
    }
  }

  @Override public CalciteOlapStatement createStatement(int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return (CalciteOlapStatement) super.createStatement(resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  @Override public CalciteOlapStatement createStatement() throws OlapException {
    try {
      return (CalciteOlapStatement) super.createStatement();
    } catch (SQLException e) {
      throw toOlap(e);
    }
  }

  @Override public void setSchema(String schema) throws OlapException {
    try {
      super.setSchema(schema);
    } catch (SQLException e) {
      throw toOlap(e);
    }
  }

  @Override public void setCatalog(String catalog) throws OlapException {
    try {
      super.setSchema(catalog);
    } catch (SQLException e) {
      throw toOlap(e);
    }
  }

  static OlapException toOlap(SQLException e) {
    if (e instanceof OlapException) {
      return (OlapException) e;
    }
    return new OlapException(e.getMessage(), e.getCause());
  }
}

// End CalciteOlapConnection.java
