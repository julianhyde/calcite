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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandHandler;
import org.apache.calcite.sql.validate.SqlValidator;

import com.google.common.collect.ImmutableMap;

import net.hydromatic.lookml.Facade;
import net.hydromatic.steelwheels.data.hsqldb.SteelwheelsHsqldb;

import java.util.LinkedHashMap;
import java.util.Map;

/** Functions related to LookML. */
public class SqlLookmlFunctions {
  private SqlLookmlFunctions() {}

  static SqlBasicFunction lookmlToSql() {
    return SqlBasicFunction.create("LOOKML_TO_SQL", ReturnTypes.CHAR,
            OperandTypes.CHARACTER_CHARACTER)
        .withOperandHandler(
            new SqlOperandHandler() {
              @Override public SqlNode rewriteCall(SqlValidator validator,
                  SqlCall call) {
                final ImmutableMap<String, SchemaFactory> schemaMap =
                    ImmutableMap.of("steelwheels",
                        SqlLookmlFunctions::createSteelwheelsSchema);
                final Facade<?> facade =
                    Facade.simple().withSchemaMap(schemaMap);
                final SqlLiteral exploreNameLiteral = call.operand(0);
                final String exploreName =
                    exploreNameLiteral.getValueAs(String.class);
                final SqlLiteral lookmlModelLiteral = call.operand(1);
                final String lookmlModel =
                    lookmlModelLiteral.getValueAs(String.class);
                final Facade.SqlQuery sqlModel =
                    facade.modelToSql(lookmlModel, exploreName);
                return SqlLiteral.createCharString(sqlModel.sql(),
                    SqlParserPos.ZERO);
              }
            });
  }

  /** Creates a Calcite schema backed by the STEELWHEELS database;
   * implements {@link org.apache.calcite.schema.SchemaFactory}. */
  static Schema createSteelwheelsSchema(SchemaPlus parent, String name,
      Map<String, Object> map) {
    Map<String, Object> map2 = new LinkedHashMap<>(map);
    map2.put("jdbcPassword", SteelwheelsHsqldb.PASSWORD);
    map2.put("jdbcUser", SteelwheelsHsqldb.USER);
    map2.put("jdbcUrl", SteelwheelsHsqldb.URI);
    map2.put("jdbcDriver", "org.hsqldb.jdbc.JDBCDriver");
    return JdbcSchema.Factory.INSTANCE.create(parent, name, map2);
  }
}
