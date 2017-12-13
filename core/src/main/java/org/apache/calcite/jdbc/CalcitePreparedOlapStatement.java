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

import org.apache.calcite.avatica.Meta;

import org.olap4j.CellSet;
import org.olap4j.CellSetMetaData;
import org.olap4j.OlapException;
import org.olap4j.OlapParameterMetaData;
import org.olap4j.PreparedOlapStatement;

import java.sql.SQLException;

/**
 * Implementation of {@link PreparedOlapStatement}
 * for the Calcite engine.
 */
@SuppressWarnings({"WeakerAccess", "unused" })
public abstract class CalcitePreparedOlapStatement
    extends CalciteJdbc41Factory.CalciteJdbc41PreparedStatement
    implements PreparedOlapStatement {
  /**
   * Creates a CalcitePreparedOlapStatement.
   *
   * @param connection Connection
   * @param h Statement handle
   * @param signature Result of preparing statement
   * @param resultSetType Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   * @throws SQLException if database error occurs
   */
  @SuppressWarnings("unused")
  protected CalcitePreparedOlapStatement(CalciteOlapConnection connection,
      Meta.StatementHandle h, Meta.Signature signature, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    super(connection, h, (CalcitePrepare.CalciteSignature) signature,
        resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override public CalciteOlapConnection getConnection() {
    return (CalciteOlapConnection) super.getConnection();
  }

  @Override public CellSetMetaData getMetaData() {
    return (CellSetMetaData) super.getMetaData();
  }

  @Override public OlapParameterMetaData getParameterMetaData()
      throws OlapException {
    try {
      return (OlapParameterMetaData) super.getParameterMetaData();
    } catch (SQLException e) {
      throw CalciteOlapConnection.toOlap(e);
    }
  }

  @Override public CellSet executeQuery() throws OlapException {
    try {
      return (CellSet) super.executeQuery();
    } catch (SQLException e) {
      throw CalciteOlapConnection.toOlap(e);
    }
  }
}

// End CalcitePreparedOlapStatement.java
