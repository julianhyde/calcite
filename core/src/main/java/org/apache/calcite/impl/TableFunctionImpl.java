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
package org.apache.calcite.impl;

import org.apache.calcite.DataContext;
import org.apache.calcite.ImplementableFunction;
import org.apache.calcite.QueryableTable;
import org.apache.calcite.SchemaPlus;
import org.apache.calcite.TableFunction;
import org.apache.calcite.impl.enumerable.CallImplementor;
import org.apache.calcite.impl.enumerable.NullPolicy;
import org.apache.calcite.impl.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.calcite.impl.enumerable.RexImpTable;
import org.apache.calcite.impl.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Implementation of {@link org.apache.calcite.TableFunction} based on a
 * method.
*/
public class TableFunctionImpl extends ReflectiveFunctionBase implements
    TableFunction, ImplementableFunction {
  private final CallImplementor implementor;

  /** Private constructor; use {@link #create}. */
  private TableFunctionImpl(Method method, CallImplementor implementor) {
    super(method);
    this.implementor = implementor;
  }

  /** Creates a {@link TableFunctionImpl} from a class, looking for an "eval"
   * method. Returns null if there is no such method. */
  public static TableFunction create(Class<?> clazz) {
    final Method method = findMethod(clazz, "eval");
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /** Creates a {@link TableFunctionImpl} from a method. */
  public static TableFunction create(final Method method) {
    if (!Modifier.isStatic(method.getModifiers())) {
      Class clazz = method.getDeclaringClass();
      if (!classHasPublicZeroArgsConstructor(clazz)) {
        throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
      }
    }
    final Class<?> returnType = method.getReturnType();
    if (!QueryableTable.class.isAssignableFrom(returnType)) {
      return null;
    }
    CallImplementor implementor = createImplementor(method);
    return new TableFunctionImpl(method, implementor);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory,
      List<Object> arguments) {
    return apply(arguments).getRowType(typeFactory);
  }

  public Type getElementType(List<Object> arguments) {
    return apply(arguments).getElementType();
  }

  public CallImplementor getImplementor() {
    return implementor;
  }

  private static CallImplementor createImplementor(final Method method) {
    return RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(
        method) {
      public Expression implement(RexToLixTranslator translator,
          RexCall call, List<Expression> translatedOperands) {
        Expression expr = super.implement(translator, call,
            translatedOperands);
        Expression queryable = Expressions.call(
          Expressions.convert_(expr, QueryableTable.class),
          BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method,
          Expressions.call(DataContext.ROOT,
            BuiltInMethod.DATA_CONTEXT_GET_QUERY_PROVIDER.method),
          Expressions.constant(null, SchemaPlus.class),
          Expressions.constant(call.getOperator().getName(),
            String.class));
        expr = Expressions.call(queryable,
            BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
        return expr;
      }
    }, NullPolicy.ANY, false);
  }

  private QueryableTable apply(List<Object> arguments) {
    try {
      Object o = null;
      if (!Modifier.isStatic(method.getModifiers())) {
        o = method.getDeclaringClass().newInstance();
      }
      //noinspection unchecked
      final Object table = method.invoke(o, arguments.toArray());
      return (QueryableTable) table;
    } catch (IllegalArgumentException e) {
      throw RESOURCE.illegalArgumentForTableFunctionCall(
          method.toString(),
          Arrays.toString(method.getParameterTypes()),
          arguments.toString()
      ).ex(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }
  }
}

// End TableFunctionImpl.java
