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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHARACTER_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PREV;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;

/**
 * Translates {@link org.apache.calcite.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 */
public class RexToLixTranslator implements RexVisitor<RexToLixTranslator.Result> {
  public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
      Util.mapOf(
          findMethod(String.class, "toUpperCase"), UPPER,
          findMethod(
              SqlFunctions.class, "substring", String.class, Integer.TYPE,
              Integer.TYPE), SUBSTRING,
          findMethod(SqlFunctions.class, "charLength", String.class),
          CHARACTER_LENGTH,
          findMethod(SqlFunctions.class, "charLength", String.class),
          CHAR_LENGTH,
          findMethod(SqlFunctions.class, "translate3", String.class, String.class,
              String.class), TRANSLATE3);

  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final RexProgram program;
  final SqlConformance conformance;
  private final Expression root;
  final RexToLixTranslator.InputGetter inputGetter;
  private final BlockBuilder list;
  private final Map<? extends RexNode, Boolean> exprNullableMap;
  private final RexToLixTranslator parent;
  private final Function1<String, InputGetter> correlates;

  private static Method findMethod(
      Class<?> clazz, String name, Class... parameterTypes) {
    try {
      return clazz.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private RexToLixTranslator(RexProgram program,
      JavaTypeFactory typeFactory,
      Expression root,
      InputGetter inputGetter,
      BlockBuilder list,
      Map<? extends RexNode, Boolean> exprNullableMap,
      RexBuilder builder,
      SqlConformance conformance,
      RexToLixTranslator parent,
      Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = Objects.requireNonNull(typeFactory);
    this.conformance = Objects.requireNonNull(conformance);
    this.root = Objects.requireNonNull(root);
    this.inputGetter = inputGetter;
    this.list = Objects.requireNonNull(list);
    this.exprNullableMap = Objects.requireNonNull(exprNullableMap);
    this.builder = Objects.requireNonNull(builder);
    this.parent = parent; // may be null
    this.correlates = correlates; // may be null
  }

  /**
   * Translates a {@link RexProgram} to a sequence of expressions and
   * declarations.
   *
   * @param program Program to be translated
   * @param typeFactory Type factory
   * @param conformance SQL conformance
   * @param list List of statements, populated with declarations
   * @param outputPhysType Output type, or null
   * @param root Root expression
   * @param inputGetter Generates expressions for inputs
   * @param correlates Provider of references to the values of correlated
   *                   variables
   * @return Sequence of expressions, optional condition
   */
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, PhysType outputPhysType, Expression root,
      InputGetter inputGetter, Function1<String, InputGetter> correlates) {
    List<Type> storageTypes = null;
    if (outputPhysType != null) {
      final RelDataType rowType = outputPhysType.getRowType();
      storageTypes = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        storageTypes.add(outputPhysType.getJavaFieldType(i));
      }
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        list, Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
        null, null)
        .setCorrelates(correlates)
        .translateList(program.getProjectList(), storageTypes);
  }

  public static Expression translateTableFunction(JavaTypeFactory typeFactory,
      SqlConformance conformance, BlockBuilder blockBuilder,
      Expression root, RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    return new RexToLixTranslator(null, typeFactory, root, null,
        blockBuilder, Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
        null, null)
        .translateTableFunction(rexCall, inputEnumerable, inputPhysType, outputPhysType);
  }

  /** Creates a translator for translating aggregate functions. */
  public static RexToLixTranslator forAggregation(JavaTypeFactory typeFactory,
      BlockBuilder list, InputGetter inputGetter, SqlConformance conformance) {
    final ParameterExpression root = DataContext.ROOT;
    return new RexToLixTranslator(null, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), new RexBuilder(typeFactory), conformance, null,
        null);
  }

  Expression translate(RexNode expr) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
    return translate(expr, nullAs, null);
  }

  Expression translate(RexNode expr, Type storageType) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs, storageType);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
      Type storageType) {
    currentStorageType = storageType;
    final Result result = expr.accept(this);
    final Expression translated =
        EnumUtils.toInternal(result.valueVariable, storageType);
    assert translated != null;
    // When we asked for not null input that would be stored as box, avoid unboxing
    if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs
        && translated.type.equals(storageType)) {
      return translated;
    }
    return nullAs.handle(translated);
  }

  Expression translateCast(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    Expression convert = null;
    switch (targetType.getSqlTypeName()) {
    case ANY:
      convert = operand;
      break;
    case DATE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_DATE.method, operand);
        break;
      case TIMESTAMP:
        convert = Expressions.convert_(
            Expressions.call(BuiltInMethod.FLOOR_DIV.method,
                operand, Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
            int.class);
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      }
      break;
    case TIME:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_TIME.method, operand);
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP:
        convert = Expressions.convert_(
            Expressions.call(
                BuiltInMethod.FLOOR_MOD.method,
                operand,
                Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
            int.class);
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      }
      break;
    case TIME_WITH_LOCAL_TIME_ZONE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method, operand);
        break;
      case TIME:
        convert = Expressions.call(
            BuiltInMethod.TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIME_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIMESTAMP:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                operand));
      }
      break;
    case TIMESTAMP:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_TIMESTAMP.method, operand);
        break;
      case DATE:
        convert = Expressions.multiply(
            Expressions.convert_(operand, long.class),
            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
        break;
      case TIME:
        convert =
            Expressions.add(
                Expressions.multiply(
                    Expressions.convert_(
                        Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                        long.class),
                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                Expressions.convert_(operand, long.class));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                Expressions.call(
                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      }
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(
                BuiltInMethod.STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                operand);
        break;
      case DATE:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    Expressions.multiply(
                        Expressions.convert_(operand, long.class),
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)))),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIME:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    Expressions.add(
                        Expressions.multiply(
                            Expressions.convert_(
                                Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                                long.class),
                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                        Expressions.convert_(operand, long.class)))),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                Expressions.call(
                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                operand));
        break;
      case TIMESTAMP:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
      }
      break;
    case BOOLEAN:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(
            BuiltInMethod.STRING_TO_BOOLEAN.method,
            operand);
      }
      break;
    case CHAR:
    case VARCHAR:
      final SqlIntervalQualifier interval =
          sourceType.getIntervalQualifier();
      switch (sourceType.getSqlTypeName()) {
      case DATE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_DATE_TO_STRING.method,
                operand));
        break;
      case TIME:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_TIME_TO_STRING.method,
                operand));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                operand));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                operand,
                Expressions.constant(interval.timeUnitRange)));
        break;
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method,
                operand,
                Expressions.constant(interval.timeUnitRange),
                Expressions.constant(
                    interval.getFractionalSecondPrecision(
                        typeFactory.getTypeSystem()))));
        break;
      case BOOLEAN:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.BOOLEAN_TO_STRING.method,
                operand));
        break;
      }
    }
    if (convert == null) {
      convert = EnumUtils.convert(operand, typeFactory.getJavaClass(targetType));
    }
    // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
    // longer than n.
    boolean pad = false;
    boolean truncate = true;
    switch (targetType.getSqlTypeName()) {
    case CHAR:
    case BINARY:
      pad = true;
      // fall through
    case VARCHAR:
    case VARBINARY:
      final int targetPrecision = targetType.getPrecision();
      if (targetPrecision >= 0) {
        switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
          // If this is a widening cast, no need to truncate.
          final int sourcePrecision = sourceType.getPrecision();
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              <= 0) {
            truncate = false;
          }
          // If this is a widening cast, no need to pad.
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              >= 0) {
            pad = false;
          }
          // fall through
        default:
          if (truncate || pad) {
            convert =
                Expressions.call(
                    pad
                        ? BuiltInMethod.TRUNCATE_OR_PAD.method
                        : BuiltInMethod.TRUNCATE.method,
                    convert,
                    Expressions.constant(targetPrecision));
          }
        }
      }
      break;
    case TIMESTAMP:
      int targetScale = targetType.getScale();
      if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
        targetScale = 0;
      }
      if (targetScale < sourceType.getScale()) {
        convert =
            Expressions.call(
                BuiltInMethod.ROUND_LONG.method,
                convert,
                Expressions.constant(
                    (long) Math.pow(10, 3 - targetScale)));
      }
      break;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      switch (sourceType.getSqlTypeName().getFamily()) {
      case NUMERIC:
        final BigDecimal multiplier = targetType.getSqlTypeName().getEndUnit().multiplier;
        final BigDecimal divider = BigDecimal.ONE;
        convert = RexImpTable.multiplyDivide(convert, multiplier, divider);
      }
    }
    return scaleIntervalToNumber(sourceType, targetType, convert);
  }

  private Expression handleNullUnboxingIfNecessary(
      Expression input,
      RexImpTable.NullAs nullAs,
      Type storageType) {
    if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs && input.type.equals(storageType)) {
      // When we asked for not null input that would be stored as box, avoid
      // unboxing which may occur in the handleNull method below.
      return input;
    }
    return handleNull(input, nullAs);
  }

  /** Adapts an expression with "normal" result to one that adheres to
   * this particular policy. Wraps the result expression into a new
   * parameter if need be.
   *
   * @param input Expression
   * @param nullAs If false, if expression is definitely not null at
   *   runtime. Therefore we can optimize. For example, we can cast to int
   *   using x.intValue().
   * @return Translated expression
   */
  public Expression handleNull(Expression input, RexImpTable.NullAs nullAs) {
    final Expression nullHandled = nullAs.handle(input);

    // If we get ConstantExpression, just return it (i.e. primitive false)
    if (nullHandled instanceof ConstantExpression) {
      return nullHandled;
    }

    // if nullHandled expression is the same as "input",
    // then we can just reuse it
    if (nullHandled == input) {
      return input;
    }

    // If nullHandled is different, then it might be unsafe to compute
    // early (i.e. unbox of null value should not happen _before_ ternary).
    // Thus we wrap it into brand-new ParameterExpression,
    // and we are guaranteed that ParameterExpression will not be shared
    String unboxVarName = "v_unboxed";
    if (input instanceof ParameterExpression) {
      unboxVarName = ((ParameterExpression) input).name + "_unboxed";
    }
    ParameterExpression unboxed = Expressions.parameter(nullHandled.getType(),
        list.newName(unboxVarName));
    list.add(Expressions.declare(Modifier.FINAL, unboxed, nullHandled));

    return unboxed;
  }

  /**
   * Handle checked Exceptions declared in Method. In such case,
   * method call should be wrapped in a try...catch block.
   * "
   *      final Type method_call;
   *      try {
   *        method_call = callExpr
   *      } catch (Exception e) {
   *        throw new RuntimeException(e);
   *      }
   * "
   */
  Expression handleMethodCheckedExceptions(Expression callExpr) {
    // Try statement
    ParameterExpression methodCall = Expressions.parameter(
        callExpr.getType(), list.newName("method_call"));
    list.add(Expressions.declare(Modifier.FINAL, methodCall, null));
    Statement st = Expressions.statement(Expressions.assign(methodCall, callExpr));
    // Catch Block, wrap checked exception in unchecked exception
    ParameterExpression e = Expressions.parameter(0, Exception.class, "e");
    Expression uncheckedException = Expressions.new_(RuntimeException.class, e);
    CatchBlock cb = Expressions.catch_(e, Expressions.throw_(uncheckedException));
    list.add(Expressions.tryCatch(st, cb));
    return methodCall;
  }

  /** Translates an expression that is not in the cache.
   *
   * @param expr Expression
   * @param nullAs If false, if expression is definitely not null at
   *   runtime. Therefore we can optimize. For example, we can cast to int
   *   using x.intValue().
   * @return Translated expression
   */
  private Expression translate0(RexNode expr, RexImpTable.NullAs nullAs,
      Type storageType) {
    if (nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable()) {
      nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
    }
    switch (expr.getKind()) {
    case INPUT_REF: {
      final int index = ((RexInputRef) expr).getIndex();
      Expression x = inputGetter.field(list, index, storageType);

      Expression input = list.append("inp" + index + "_", x); // safe to share
      return handleNullUnboxingIfNecessary(input, nullAs, storageType);
    }
    case PATTERN_INPUT_REF: {
      final int index = ((RexInputRef) expr).getIndex();
      Expression x = inputGetter.field(list, index, storageType);

      Expression input = list.append("inp" + index + "_", x); // safe to share
      return handleNullUnboxingIfNecessary(input, nullAs, storageType);
    }
    case LOCAL_REF:
      return translate(
          deref(expr),
          nullAs,
          storageType);
    case LITERAL:
      return translateLiteral(
          (RexLiteral) expr,
          nullifyType(
              expr.getType(),
              isNullable(expr)
                  && nullAs != RexImpTable.NullAs.NOT_POSSIBLE),
          typeFactory,
          nullAs);
    case DYNAMIC_PARAM:
      return translateParameter((RexDynamicParam) expr, nullAs, storageType);
    case CORREL_VARIABLE:
      throw new RuntimeException("Cannot translate " + expr + ". Correlated"
          + " variables should always be referenced by field access");
    case FIELD_ACCESS: {
      RexFieldAccess fieldAccess = (RexFieldAccess) expr;
      RexNode target = deref(fieldAccess.getReferenceExpr());
      int fieldIndex = fieldAccess.getField().getIndex();
      String fieldName = fieldAccess.getField().getName();
      switch (target.getKind()) {
      case CORREL_VARIABLE:
        if (correlates == null) {
          throw new RuntimeException("Cannot translate " + expr + " since "
              + "correlate variables resolver is not defined");
        }
        InputGetter getter =
            correlates.apply(((RexCorrelVariable) target).getName());
        Expression y = getter.field(list, fieldIndex, storageType);
        Expression input = list.append("corInp" + fieldIndex + "_", y); // safe to share
        return handleNullUnboxingIfNecessary(input, nullAs, storageType);
      default:
        RexNode rxIndex = builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
        RexNode rxName = builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
        RexCall accessCall = (RexCall) builder.makeCall(
            fieldAccess.getType(),
            SqlStdOperatorTable.STRUCT_ACCESS,
            ImmutableList.of(target, rxIndex, rxName));
        return translateCall(accessCall, nullAs);
      }
    }
    default:
      if (expr instanceof RexCall) {
        return translateCall((RexCall) expr, nullAs);
      }
      throw new RuntimeException(
          "cannot translate expression " + expr);
    }
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = program.getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Translates a call to an operator or function. */
  private Expression translateCall(RexCall call, RexImpTable.NullAs nullAs) {
    final SqlOperator operator = call.getOperator();
    CallImplementor implementor = null;
//        RexImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    return implementor.implement(this, call, nullAs);
  }

  /** Translates a parameter. */
  private Expression translateParameter(RexDynamicParam expr,
      RexImpTable.NullAs nullAs, Type storageType) {
    if (storageType == null) {
      storageType = typeFactory.getJavaClass(expr.getType());
    }
    return nullAs.handle(
        EnumUtils.convert(
            Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant("?" + expr.getIndex())),
            storageType));
  }

  /** Translates a literal.
   *
   * @throws AlwaysNull if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
      case TRUE:
      case IS_NULL:
        return RexImpTable.TRUE_EXPR;
      case FALSE:
      case IS_NOT_NULL:
        return RexImpTable.FALSE_EXPR;
      case NOT_POSSIBLE:
        throw AlwaysNull.INSTANCE;
      case NULL:
      default:
        return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
      case IS_NOT_NULL:
        return RexImpTable.TRUE_EXPR;
      case IS_NULL:
        return RexImpTable.FALSE_EXPR;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
    final Object value2;
    switch (literal.getType().getSqlTypeName()) {
    case DECIMAL:
      final BigDecimal bd = literal.getValueAs(BigDecimal.class);
      if (javaClass == float.class) {
        return Expressions.constant(bd, javaClass);
      } else if (javaClass == double.class) {
        return Expressions.constant(bd, javaClass);
      }
      assert javaClass == BigDecimal.class;
      return Expressions.new_(BigDecimal.class,
          Expressions.constant(bd.toString()));
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      value2 = literal.getValueAs(Integer.class);
      javaClass = int.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      value2 = literal.getValueAs(Long.class);
      javaClass = long.class;
      break;
    case CHAR:
    case VARCHAR:
      value2 = literal.getValueAs(String.class);
      break;
    case BINARY:
    case VARBINARY:
      return Expressions.new_(
          ByteString.class,
          Expressions.constant(
              literal.getValueAs(byte[].class),
              byte[].class));
    case SYMBOL:
      value2 = literal.getValueAs(Enum.class);
      javaClass = value2.getClass();
      break;
    default:
      final Primitive primitive = Primitive.ofBoxOr(javaClass);
      final Comparable value = literal.getValueAs(Comparable.class);
      if (primitive != null && value instanceof Number) {
        value2 = primitive.number((Number) value);
      } else {
        value2 = value;
      }
    }
    return Expressions.constant(value2, javaClass);
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs) {
    return translateList(operandList, nullAs,
        EnumUtils.internalTypes(operandList));
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>();
    for (Pair<RexNode, ? extends Type> e : Pair.zip(operandList, storageTypes)) {
      list.add(translate(e.left, nullAs, e.right));
    }
    return list;
  }

  /**
   * Translates the list of {@code RexNode}, using the default output types.
   * This might be suboptimal in terms of additional box-unbox when you use
   * the translation later.
   * If you know the java class that will be used to store the results, use
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator#translateList(java.util.List, java.util.List)}
   * version.
   *
   * @param operandList list of RexNodes to translate
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList) {
    return translateList(operandList, EnumUtils.internalTypes(operandList));
  }

  /**
   * Translates the list of {@code RexNode}, while optimizing for output
   * storage.
   * For instance, if the result of translation is going to be stored in
   * {@code Object[]}, and the input is {@code Object[]} as well,
   * then translator will avoid casting, boxing, etc.
   *
   * @param operandList list of RexNodes to translate
   * @param storageTypes hints of the java classes that will be used
   *                     to store translation results. Use null to use
   *                     default storage type
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());

    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;
  }

  private Expression translateTableFunction(RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    assert rexCall.getOperator() instanceof SqlWindowTableFunction;
    TableFunctionCallImplementor implementor =
        RexImpTable.INSTANCE.get((SqlWindowTableFunction) rexCall.getOperator());
    if (implementor == null) {
      throw Util.needToImplement("implementor of " + rexCall.getOperator().getName());
    }
    return implementor.implement(
        this, inputEnumerable, rexCall, inputPhysType, outputPhysType);
  }

  public static Expression translateCondition(RexProgram program,
      JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter,
      Function1<String, InputGetter> correlates, SqlConformance conformance) {
    if (program.getCondition() == null) {
      return RexImpTable.TRUE_EXPR;
    }
    final ParameterExpression root = DataContext.ROOT;
    RexToLixTranslator translator =
        new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
            null, null);
    translator = translator.setCorrelates(correlates);
    return translator.translate(
        program.getCondition(),
        RexImpTable.NullAs.FALSE);
  }

  public Expression translateConstructor(
      List<RexNode> operandList, SqlKind kind) {
    switch (kind) {
    case MAP_VALUE_CONSTRUCTOR:
      Expression map =
          list.append(
              "map",
              Expressions.new_(LinkedHashMap.class),
              false);
      for (int i = 0; i < operandList.size(); i++) {
        RexNode key = operandList.get(i++);
        RexNode value = operandList.get(i);
        list.add(
            Expressions.statement(
                Expressions.call(
                    map,
                    BuiltInMethod.MAP_PUT.method,
                    Expressions.box(translate(key)),
                    Expressions.box(translate(value)))));
      }
      return map;
    case ARRAY_VALUE_CONSTRUCTOR:
      Expression lyst =
          list.append(
              "list",
              Expressions.new_(ArrayList.class),
              false);
      for (RexNode value : operandList) {
        list.add(
            Expressions.statement(
                Expressions.call(
                    lyst,
                    BuiltInMethod.COLLECTION_ADD.method,
                    Expressions.box(translate(value)))));
      }
      return lyst;
    default:
      throw new AssertionError("unexpected: " + kind);
    }
  }

  /** Returns whether an expression is nullable. Even if its type says it is
   * nullable, if we have previously generated a check to make sure that it is
   * not null, we will say so.
   *
   * <p>For example, {@code WHERE a == b} translates to
   * {@code a != null && b != null && a.equals(b)}. When translating the
   * 3rd part of the disjunction, we already know a and b are not null.</p>
   *
   * @param e Expression
   * @return Whether expression is nullable in the current translation context
   */
  public boolean isNullable(RexNode e) {
    if (!e.getType().isNullable()) {
      return false;
    }
    final Boolean b = isKnownNullable(e);
    return b == null || b;
  }

  /**
   * Walks parent translator chain and verifies if the expression is nullable.
   *
   * @param node RexNode to check if it is nullable or not
   * @return null when nullability is not known, true or false otherwise
   */
  protected Boolean isKnownNullable(RexNode node) {
    if (!exprNullableMap.isEmpty()) {
      Boolean nullable = exprNullableMap.get(node);
      if (nullable != null) {
        return nullable;
      }
    }
    return parent == null ? null : parent.isKnownNullable(node);
  }

  /** Creates a read-only copy of this translator that records that a given
   * expression is nullable. */
  public RexToLixTranslator setNullable(RexNode e, boolean nullable) {
    return setNullable(Collections.singletonMap(e, nullable));
  }

  /** Creates a read-only copy of this translator that records that a given
   * expression is nullable. */
  public RexToLixTranslator setNullable(
      Map<? extends RexNode, Boolean> nullable) {
    if (nullable == null || nullable.isEmpty()) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        nullable, builder, conformance, this, correlates);
  }

  public RexToLixTranslator setBlock(BlockBuilder block) {
    if (block == list) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        block, ImmutableMap.of(), builder, conformance, this, correlates);
  }

  public RexToLixTranslator setCorrelates(
      Function1<String, InputGetter> correlates) {
    if (this.correlates == correlates) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), builder, conformance, this, correlates);
  }

  private RexToLixTranslator withConformance(SqlConformance conformance) {
    if (conformance == this.conformance) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), builder, conformance, this, correlates);
  }

  public RelDataType nullifyType(RelDataType type, boolean nullable) {
    if (!nullable) {
      final Primitive primitive = javaPrimitive(type);
      if (primitive != null) {
        return typeFactory.createJavaType(primitive.primitiveClass);
      }
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  private Primitive javaPrimitive(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      return Primitive.ofBox(
          ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
    }
    return null;
  }

  public Expression getRoot() {
    return root;
  }

  private static Expression scaleIntervalToNumber(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    switch (targetType.getSqlTypeName().getFamily()) {
    case NUMERIC:
      switch (sourceType.getSqlTypeName()) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        // Scale to the given field.
        final BigDecimal multiplier = BigDecimal.ONE;
        final BigDecimal divider =
            sourceType.getSqlTypeName().getEndUnit().multiplier;
        return RexImpTable.multiplyDivide(operand, multiplier, divider);
      }
    }
    return operand;
  }

  /** Translates a field of an input to an expression. */
  public interface InputGetter {
    Expression field(BlockBuilder list, int index, Type storageType);
  }

  /** Implementation of {@link InputGetter} that calls
   * {@link PhysType#fieldReference}. */
  public static class InputGetterImpl implements InputGetter {
    private List<Pair<Expression, PhysType>> inputs;

    public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
      this.inputs = inputs;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      int offset = 0;
      for (Pair<Expression, PhysType> input : inputs) {
        final PhysType physType = input.right;
        int fieldCount = physType.getRowType().getFieldCount();
        if (index >= offset + fieldCount) {
          offset += fieldCount;
          continue;
        }
        final Expression left = list.append("current", input.left);
        return physType.fieldReference(left, index - offset, storageType);
      }
      throw new IllegalArgumentException("Unable to find field #" + index);
    }
  }

  /** Thrown in the unusual (but not erroneous) situation where the expression
   * we are translating is the null literal but we have already checked that
   * it is not null. It is easier to throw (and caller will always handle)
   * than to check exhaustively beforehand. */
  static class AlwaysNull extends ControlFlowException {
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final AlwaysNull INSTANCE = new AlwaysNull();

    private AlwaysNull() {}
  }

  /**
   * Result of translating a {@code RexNode}
   */
  public static class Result {
    final ParameterExpression isNullVariable;
    final ParameterExpression valueVariable;

    public Result(ParameterExpression isNullVariable,
        ParameterExpression valueVariable) {
      this.isNullVariable = isNullVariable;
      this.valueVariable = valueVariable;
    }
  }

  /**
   * Map from RexLiteral's variable name to its literal, which is often a
   * ({@link org.apache.calcite.linq4j.tree.ConstantExpression}))
   * It is used in the some {@code RexCall}'s implementors, such as
   * {@code ExtractImplementor}
   */
  private final Map<Expression, Expression> literalMap = new HashMap<>();

  /**
   * For {@code RexCall}, keep the list of its operand's {@code Result}.
   * It is useful when creating a {@code CallImplementor}
   */
  private final Map<RexCall, List<Result>> callOperandResultMap = new HashMap<>();

  /**
   * Map from RexNode under specific storage type to its Result,
   * to avoid generating duplicate code.
   * For {@code RexInputRef}, {@code RexDynamicParam} and {@code RexFieldAccess}
   */
  private final Map<Pair<RexNode, Type>, Result> rexWithStorageTypeResultMap = new HashMap<>();

  /**
   * Map from RexNode to its Result, to avoid generating duplicate code
   * For {@code RexLiteral} and {@code RexCall}
   */
  private final Map<RexNode, Result> rexResultMap = new HashMap<>();

  private Type currentStorageType = null;

  /**
   * Visit {@code RexInputRef}. If it has never been visited
   * under current storage type before, {@code RexToLixTranslator}
   * generally produces three lines of code.
   * For example, when visiting a column (named commission) in
   * table Employee, the generated code snippet is:
   * {@code
   *   final Employee current =(Employee) inputEnumerator.current();
       final Integer input_value = current.commission;
       final boolean input_isNull = input_value == null;
   * }
   */
  @Override public Result visitInputRef(RexInputRef inputRef) {
    final Pair<RexNode, Type> key = Pair.of(inputRef, currentStorageType);
    // If the RexInputRef has been visited under current storage type already,
    // it is not necessary to visit it again, just return the result.
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    // Generate one line of code to get the input, e.g.,
    // "final Employee current =(Employee) inputEnumerator.current();"
    final Expression valueExpression = inputGetter.field(
        list, inputRef.getIndex(), currentStorageType);

    // Generate one line of code for the value of RexInputRef, e.g.,
    // "final Integer input_value = current.commission;"
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(), list.newName("input_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));

    // Generate one line of code to check whether RexInputRef is null, e.g.,
    // "final boolean input_isNull = input_value == null;"
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    final Result result = new Result(isNullVariable, valueVariable);

    // Cache <RexInputRef, currentStorageType>'s result
    // Note: EnumerableMatch's PrevInputGetter changes index each time,
    // it is not right to reuse the result under such case.
    if (!(inputGetter instanceof EnumerableMatch.PrevInputGetter)) {
      rexWithStorageTypeResultMap.put(key, result);
    }
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitLocalRef(RexLocalRef localRef) {
    return deref(localRef).accept(this);
  }

  /**
   * Visit {@code RexLiteral}. If it has never been visited before,
   * {@code RexToLixTranslator} will generate two lines of code. For example,
   * when visiting a primitive int (10), the generated code snippet is:
   * {@code
   *   final int literal_value = 10;
   *   final boolean literal_isNull = false;
   * }
   */
  @Override public Result visitLiteral(RexLiteral literal) {
    // If the RexLiteral has been visited already, just return the result
    if (rexResultMap.containsKey(literal)) {
      return rexResultMap.get(literal);
    }
    // Generate one line of code for the value of RexLiteral, e.g.,
    // "final int literal_value = 10;"
    final Expression valueExpression = literal.isNull()
        // Note: even for null literal, we can't loss its type information
        ? getTypedNullLiteral(literal)
        : translateLiteral(literal, literal.getType(),
            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(),
            list.newName("literal_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));

    // Generate one line of code to check whether RexLiteral is null, e.g.,
    // "final boolean literal_isNull = false;"
    final Expression isNullExpression =
        literal.isNull() ? RexImpTable.TRUE_EXPR : RexImpTable.FALSE_EXPR;
    final ParameterExpression isNullVariable = Expressions.parameter(
        Boolean.TYPE, list.newName("literal_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    // Maintain the map from valueVariable (ParameterExpression) to real Expression
    literalMap.put(valueVariable, valueExpression);
    final Result result = new Result(isNullVariable, valueVariable);
    // Cache RexLiteral's result
    rexResultMap.put(literal, result);
    return result;
  }

  /**
   * Get the {@code Expression} for null literal without loss its type information
   */
  private Expression getTypedNullLiteral(RexLiteral literal) {
    assert literal.isNull();
    Type javaClass = typeFactory.getJavaClass(literal.getType());
    switch (literal.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      javaClass = Integer.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      javaClass = Long.class;
      break;
    }
    return javaClass == null
        ? RexImpTable.NULL_EXPR
        : Expressions.constant(null, javaClass);
  }

  /**
   * Visit {@code RexCall}. For most {@code SqlOperator}s, we can get the implementor
   * from {@code RexImpTable}. Several operators (e.g., CaseWhen) with special semantics
   * need to be implemented separately.
   */
  @Override public Result visitCall(RexCall call) {
    if (rexResultMap.containsKey(call)) {
      return rexResultMap.get(call);
    }
    final SqlOperator operator = call.getOperator();
    if (operator == PREV) {
      return implementPrev(call);
    }
    if (operator == CASE) {
      return implementCaseWhen(call);
    }
    final RexImpTable.RexCallImplementor implementor =
        RexImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    final List<RexNode> operandList = call.getOperands();
    final List<Type> storageTypes = EnumUtils.internalTypes(operandList);
    final List<Result> operandResults = new ArrayList<>();
    for (int i = 0; i < operandList.size(); i++) {
      final Result operandResult =
          implementCallOperand(operandList.get(i), storageTypes.get(i), this);
      operandResults.add(operandResult);
    }
    callOperandResultMap.put(call, operandResults);
    final Result result = implementor.implement(this, call, operandResults);
    rexResultMap.put(call, result);
    return result;
  }

  private static Result implementCallOperand(final RexNode operand,
      final Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    Result operandResult = operand.accept(translator);
    if (storageType != null) {
      operandResult = translator.toInnerStorageType(operandResult, storageType);
    }
    translator.currentStorageType = originalStorageType;
    return operandResult;
  }

  private static Expression implementCallOperand2(final RexNode operand,
      final Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    final Expression result =  translator.translate(operand);
    translator.currentStorageType = originalStorageType;
    return result;
  }

  /**
   * For {@code PREV} operator, the offset of {@code inputGetter}
   * should be set first
   */
  private Result implementPrev(RexCall call) {
    final RexNode node = call.getOperands().get(0);
    final RexNode offset = call.getOperands().get(1);
    final Expression offs = Expressions.multiply(translate(offset),
            Expressions.constant(-1));
    ((EnumerableMatch.PrevInputGetter) inputGetter).setOffset(offs);
    return node.accept(this);
  }

  /**
   * The CASE operator is SQL’s way of handling if/then logic.
   * Different with other {@code RexCall}s, it is not safe to
   * implement its operands first.
   * For example: {@code
   *   select case when s=0 then false
   *          else 100/s > 0 end
   *   from (values (1),(0)) ax(s);
   * }
   */
  private Result implementCaseWhen(RexCall call) {
    final Type returnType = typeFactory.getJavaClass(call.getType());
    final ParameterExpression valueVariable =
        Expressions.parameter(returnType,
            list.newName("case_when_value"));
    list.add(Expressions.declare(0, valueVariable, null));
    final List<RexNode> operandList = call.getOperands();
    implementRecursively(this, operandList, valueVariable, 0);
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("case_when_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    final Result result = new Result(isNullVariable, valueVariable);
    rexResultMap.put(call, result);
    return result;
  }

  /**
   * Case statements of the form:
   * {@code CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END}.
   * When {@code a = true}, returns {@code b};
   * when {@code c = true}, returns {@code d};
   * else returns {@code e}.
   *
   * We generate code that looks like: {@code
   *      int case_when_value;
   *      ......code for a......
   *      if (!a_isNull && a_value) {
   *          ......code for b......
   *          case_when_value = res(b_isNull, b_value);
   *      } else {
   *          ......code for c......
   *          if (!c_isNull && c_value) {
   *              ......code for d......
   *              case_when_value = res(d_isNull, d_value);
   *          } else {
   *              ......code for e......
   *              case_when_value = res(e_isNull, e_value);
   *          }
   *      }
   * }
   */
  private void implementRecursively(final RexToLixTranslator currentTranslator,
      final List<RexNode> operandList, final ParameterExpression valueVariable, int pos) {
    final BlockBuilder currentBlockBuilder = currentTranslator.getBlockBuilder();
    final List<Type> storageTypes = EnumUtils.internalTypes(operandList);
    // [ELSE] clause
    if (pos == operandList.size() - 1) {
      Expression res = implementCallOperand2(operandList.get(pos),
          storageTypes.get(pos), currentTranslator);
      currentBlockBuilder.add(
          Expressions.statement(
              Expressions.assign(valueVariable,
                  EnumUtils.convert(res, valueVariable.getType()))));
      return;
    }
    // Condition code: !a_isNull && a_value
    final RexNode testerNode = operandList.get(pos);
    final Result testerResult = implementCallOperand(testerNode,
        storageTypes.get(pos), currentTranslator);
    final Expression tester = Expressions.andAlso(
        Expressions.not(testerResult.isNullVariable),
        testerResult.valueVariable);
    // Code for {if} branch
    final RexNode ifTrueNode = operandList.get(pos + 1);
    final BlockBuilder ifTrueBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifTrueTranslator =
        currentTranslator.setBlock(ifTrueBlockBuilder);
    final Expression ifTrueRes = implementCallOperand2(ifTrueNode,
        storageTypes.get(pos + 1), ifTrueTranslator);
    // Assign the value: case_when_value = ifTrueRes
    ifTrueBlockBuilder.add(
        Expressions.statement(
            Expressions.assign(valueVariable,
                EnumUtils.convert(ifTrueRes, valueVariable.getType()))));
    final BlockStatement ifTrue = ifTrueBlockBuilder.toBlock();
    // There is no [ELSE] clause
    if (pos + 1 == operandList.size() - 1) {
      currentBlockBuilder.add(
          Expressions.ifThen(tester, ifTrue));
      return;
    }
    // Generate code for {else} branch recursively
    final BlockBuilder ifFalseBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifFalseTranslator =
        currentTranslator.setBlock(ifFalseBlockBuilder);
    implementRecursively(ifFalseTranslator, operandList, valueVariable, pos + 2);
    final BlockStatement ifFalse = ifFalseBlockBuilder.toBlock();
    currentBlockBuilder.add(
        Expressions.ifThenElse(tester, ifTrue, ifFalse));
  }

  private Result toInnerStorageType(final Result result, final Type storageType) {
    final Expression valueExpression =
        EnumUtils.toInternal(result.valueVariable, storageType);
    if (valueExpression.equals(result.valueVariable)) {
      return result;
    }
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(),
            list.newName(result.valueVariable.name + "_inner_type"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable = result.isNullVariable;
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitDynamicParam(RexDynamicParam dynamicParam) {
    final Pair<RexNode, Type> key = Pair.of(dynamicParam, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final Type storageType = currentStorageType != null
        ? currentStorageType : typeFactory.getJavaClass(dynamicParam.getType());
    final Expression valueExpression = EnumUtils.convert(
        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
            Expressions.constant("?" + dynamicParam.getIndex())),
        storageType);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(), list.newName("value_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, checkNull(valueVariable)));
    final Result result = new Result(isNullVariable, valueVariable);
    rexWithStorageTypeResultMap.put(key, result);
    return result;
  }

  @Override public Result visitFieldAccess(RexFieldAccess fieldAccess) {
    final Pair<RexNode, Type> key = Pair.of(fieldAccess, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final RexNode target = deref(fieldAccess.getReferenceExpr());
    int fieldIndex = fieldAccess.getField().getIndex();
    String fieldName = fieldAccess.getField().getName();
    switch (target.getKind()) {
    case CORREL_VARIABLE:
      if (correlates == null) {
        throw new RuntimeException("Cannot translate " + fieldAccess
            + " since correlate variables resolver is not defined");
      }
      final RexToLixTranslator.InputGetter getter =
          correlates.apply(((RexCorrelVariable) target).getName());
      final Expression input = getter.field(
          list, fieldIndex, currentStorageType);
      final Expression condition = checkNull(input);
      final ParameterExpression valueVariable =
          Expressions.parameter(input.getType(), list.newName("corInp_value"));
      list.add(Expressions.declare(Modifier.FINAL, valueVariable, input));
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"));
      final Expression isNullExpression = Expressions.condition(
          condition,
          RexImpTable.TRUE_EXPR,
          checkNull(valueVariable));
      list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      final Result result1 = new Result(isNullVariable, valueVariable);
      rexWithStorageTypeResultMap.put(key, result1);
      return result1;
    default:
      RexNode rxIndex =
          builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
      RexNode rxName =
          builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
      RexCall accessCall = (RexCall) builder.makeCall(
          fieldAccess.getType(), SqlStdOperatorTable.STRUCT_ACCESS,
          ImmutableList.of(target, rxIndex, rxName));
      final Result result2 = accessCall.accept(this);
      rexWithStorageTypeResultMap.put(key, result2);
      return result2;
    }
  }

  @Override public Result visitOver(RexOver over) {
    throw new RuntimeException("cannot translate expression " + over);
  }

  @Override public Result visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Cannot translate " + correlVariable
        + ". Correlated variables should always be referenced by field access");
  }

  @Override public Result visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("cannot translate expression " + rangeRef);
  }

  @Override public Result visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("cannot translate expression " + subQuery);
  }

  @Override public Result visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  @Override public Result visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return visitInputRef(fieldRef);
  }

  Expression checkNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.FALSE_EXPR;
    }
    return Expressions.equal(expr, RexImpTable.NULL_EXPR);
  }

  Expression checkNotNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.TRUE_EXPR;
    }
    return Expressions.notEqual(expr, RexImpTable.NULL_EXPR);
  }

  BlockBuilder getBlockBuilder() {
    return list;
  }

  Expression getLiteral(Expression literalVariable) {
    return literalMap.get(literalVariable);
  }

  List<Result> getCallOperandResult(RexCall call) {
    return callOperandResultMap.get(call);
  }
}
