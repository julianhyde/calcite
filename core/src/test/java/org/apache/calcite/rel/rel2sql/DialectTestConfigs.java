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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Util;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.function.Supplier;

/** Utilities for {@link DialectTestConfig}. */
class DialectTestConfigs {
  private DialectTestConfigs() {
  }

  static final Supplier<DialectTestConfig> INSTANCE_SUPPLIER =
      Suppliers.memoize(() -> {
        final ImmutableList.Builder<DialectTestConfig.Dialect> b =
            ImmutableList.builder();
        for (DialectCode dialectCode : DialectCode.values()) {
          b.add(dialectCode.toDialect());
        }
        final ImmutableList<DialectTestConfig.Dialect> list = b.build();
        final Iterable<String> dialectNames =
            Util.transform(list, dialect -> dialect.name);
        if (!Ordering.natural().isOrdered(dialectNames)) {
          throw new AssertionError("not ordered: " + dialectNames);
        }
        return DialectTestConfig.of(list);
      })::get;

  static MysqlSqlDialect mysqlDialect(NullCollation nullCollation) {
    return new MysqlSqlDialect(MysqlSqlDialect.DEFAULT_CONTEXT
        .withNullCollation(nullCollation));
  }

  static SqlDialect oracleDialect(final int maxVarcharLength) {
    return new OracleSqlDialect(OracleSqlDialect.DEFAULT_CONTEXT
        .withDataTypeSystem(new RelDataTypeSystemImpl() {
          @Override public int getMaxPrecision(SqlTypeName typeName) {
            switch (typeName) {
            case VARCHAR:
              return maxVarcharLength;
            default:
              return super.getMaxPrecision(typeName);
            }
          }
        }));
  }

  static SqlDialect postgresqlDialect(final int maxVarcharLength) {
    return new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT
        .withDataTypeSystem(new RelDataTypeSystemImpl() {
          @Override public int getMaxPrecision(SqlTypeName typeName) {
            switch (typeName) {
            case VARCHAR:
              return maxVarcharLength;
            default:
              return super.getMaxPrecision(typeName);
            }
          }
        }));
  }

  /** Creates a dialect for Microsoft SQL Server.
   *
   * <p>MSSQL 2008 has version 10.0, 2012 has 11.0, 2017 has 14.0. */
  static SqlDialect mssqlDialect(int majorVersion) {
    final SqlDialect mssqlDialect =
        SqlDialect.DatabaseProduct.MSSQL.getDialect();
    return new MssqlSqlDialect(MssqlSqlDialect.DEFAULT_CONTEXT
        .withDatabaseMajorVersion(majorVersion)
        .withIdentifierQuoteString(mssqlDialect.quoteIdentifier("")
            .substring(0, 1))
        .withNullCollation(mssqlDialect.getNullCollation()));
  }

  /** Creates a dialect that doesn't treat integer literals in the ORDER BY as
   * field references. */
  static SqlDialect nonOrdinalDialect() {
    return new SqlDialect(SqlDialect.EMPTY_CONTEXT) {
      @Override public SqlConformance getConformance() {
        return SqlConformanceEnum.STRICT_99;
      }
    };
  }

  static final Supplier<SqlDialect> JETHRO_DIALECT_SUPPLIER =
      Suppliers.memoize(() ->
          new JethroDataSqlDialect(
              SqlDialect.EMPTY_CONTEXT
                  .withDatabaseProduct(SqlDialect.DatabaseProduct.JETHRO)
                  .withDatabaseMajorVersion(1)
                  .withDatabaseMinorVersion(0)
                  .withDatabaseVersion("1.0")
                  .withIdentifierQuoteString("\"")
                  .withNullCollation(NullCollation.HIGH)
                  .withJethroInfo(JethroDataSqlDialect.JethroInfo.EMPTY)));
}
