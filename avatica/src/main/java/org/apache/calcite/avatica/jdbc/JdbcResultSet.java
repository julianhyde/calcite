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
package org.apache.calcite.avatica.jdbc;

import org.apache.calcite.avatica.Meta;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.avatica.Meta.MetaResultSet}
 *  upon a JDBC {@link java.sql.ResultSet}.
 *
 *  @see org.apache.calcite.avatica.jdbc.JdbcMeta */
class JdbcResultSet extends Meta.MetaResultSet {
  protected JdbcResultSet(int statementId, boolean ownStatement,
      Meta.Signature signature, Iterable<Object> iterable) {
    super(statementId, ownStatement, signature, iterable);
  }

  /** Creates a result set. */
  public static JdbcResultSet create(ResultSet resultSet) {
    try {
      int id = resultSet.getStatement().hashCode();
      Meta.Signature sig = JdbcMeta.signature(resultSet.getMetaData());
      final int columnCount = resultSet.getMetaData().getColumnCount();
      final List<Object> a = new ArrayList<>();
      while (resultSet.next()) {
        Object[] o = new Object[columnCount];
        for (int i = 1; i <= columnCount; i++) {
          o[i - 1] = resultSet.getObject(i);
        }
        a.add(o);
      }
      resultSet.close();
      return new JdbcResultSet(id, true, sig, a);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

// End JdbcResultSet.java
