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
package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Rule that transforms a row-expression.
 *
 * <p>The analog for relational expressions is {@link RelRule}.
 *
 * @see RexRuleProgram
 */
public abstract class RexRule {

  /** Creates a rule that simplifies a {@link RexNode} from two lambdas. */
  public static RexRule of(Function<OperandBuilder, Done> descriptor,
      RexNodeTransformer transformer) {
    return new RexRule() {
      @Override public Done describe(OperandBuilder b) {
        return descriptor.apply(b);
      }

      @Override public RexNode apply(Context cx, RexNode e,
          RexUnknownAs unknownAs) {
        return transformer.apply(cx, e);
      }
    };
  }

  /** Creates a rule that simplifies a {@link RexCall} from two lambdas.
   *
   * <p>You can write rules of this type for strong operators (those that
   * return NULL if and only if one of their arguments return NULL).
   * If an operator is not strong, you should use {@link #ofWeakCall}
   * and interface {@link RexWeakCallTransformer}. */
  public static RexRule ofCall(Function<OperandBuilder, Done> descriptor,
      RexCallTransformer transformer) {
    return new RexRule() {
      @Override public Done describe(OperandBuilder b) {
        return descriptor.apply(b);
      }

      @Override public RexNode apply(Context cx, RexNode e,
          RexUnknownAs unknownAs) {
        return transformer.apply(cx, (RexCall) e);
      }
    };
  }

  /** Creates a rule that simplifies a {@link RexCall} to a non-strong operator
   * from two lambdas. */
  public static RexRule ofWeakCall(Function<OperandBuilder, Done> descriptor,
      RexWeakCallTransformer transformer) {
    return new RexRule() {
      @Override public Done describe(OperandBuilder b) {
        return descriptor.apply(b);
      }

      @Override public RexNode apply(Context cx, RexNode e,
          RexUnknownAs unknownAs) {
        return transformer.apply(cx, (RexCall) e, unknownAs);
      }
    };
  }

  /** Asks the rule to build a description of what operands it matches.
   *
   * <p>That description will be used by the {@link RexRuleProgram} to index
   * its rules, calling only those rules that may match a given expression. */
  public abstract Done describe(OperandBuilder b);

  /** Applies this rule to an expression.
   *
   * <p>Returns the transformed expression, or {@code e} if the rule does not
   * apply. */
  public abstract RexNode apply(Context cx, RexNode e, RexUnknownAs unknownAs);

  /** Context in which a RexRule is applied. */
  public interface Context {
    default RelDataTypeFactory typeFactory() {
      return rexBuilder().typeFactory;
    }

    RexBuilder rexBuilder();

    RexExecutor executor();

    RexUnknownAs unknownAs();

    Context withPredicates(RelOptPredicateList predicateList);

    RexNode simplify(RexNode e, RexUnknownAs unknownAs);

    RexNode simplify(RexNode e);

    RexNode isTrue(RexNode e);
  }

  /** Function that creates an operand.
   *
   * @see OperandDetailBuilder#inputs(OperandTransform...) */
  @FunctionalInterface
  public interface OperandTransform extends Function<OperandBuilder, Done> {
  }

  /** Callback to create an operand.
   *
   * @see RexRule#describe */
  public interface OperandBuilder {
    /** Matches any operand. */
    Done any();

    /** Matches any operand that is not null. */
    Done notNull();

    /** Matches any {@link RexLiteral} operand. */
    default Done isLiteral() {
      return isLiteral(literal -> true);
    }

    /** Matches a {@link RexLiteral} operand that matches a given predicate. */
    Done isLiteral(Predicate<RexLiteral> predicate);

    /** Indicates that the rule matches a {@link RexNode} of a given
     * {@link SqlKind} or kinds. The matched expression may or may not be a
     * {@link RexCall}; if not a {@code RexCall}, it will have zero inputs. */
    OperandDetailBuilder ofKind(SqlKind... kinds);

    /** Indicates that the rule matches a {@link RexCall} to a given
     * {@link SqlOperator} instance. */
    OperandDetailBuilder callTo(SqlOperator operator);

    /** Indicates that the RexRule matches a {@link RexCall} to an
     * {@link SqlOperator} that is an instance of a given class
     * (or sub-class). */
    OperandDetailBuilder callTo(Class<? extends SqlOperator> operatorClass);
  }

  /** Indicates that an operand is complete.
   *
   * @see OperandBuilder */
  public interface Done {
  }

  /** Add details about an operand, such as its inputs. */
  public interface OperandDetailBuilder {
    /** Sets the predicate of this call. */
    OperandDetailBuilder predicate(Predicate<? super RexCall> predicate);

    /** Indicates that this call takes no operands. */
    default Done noInputs() {
      return inputs();
    }

    /** Indicates that this call has a single operand. */
    default Done oneInput(OperandTransform transform) {
      return inputs(transform);
    }

    /** Indicates that this call has several operands. */
    Done inputs(OperandTransform... transforms);

    /** Indicates that this call has several overloads.
     *
     * <p>For example, the following matches a call to operator X with 2, 3 or 4
     * arguments:
     *
     * <blockquote><pre>
     *   b.callTo(SqlKind.X)
     *       .overloadedInputs(RexRule::any, RexRule::any)
     *       .overloadedInputs(RexRule::any, RexRule::any, RexRule::any)
     *       .inputs(RexRule::any, RexRule::any, RexRule::any, RexRule::any)
     * </pre></blockquote>
     */
    OperandDetailBuilder overloadedInputs(OperandTransform... transforms);

    /** Indicates that this call takes any number or type of operands. */
    Done anyInputs();
  }

  /** Transforms a {@link RexNode}. */
  @FunctionalInterface
  public interface RexNodeTransformer
      extends BiFunction<Context, RexNode, RexNode> {
  }

  /** Transforms a {@link RexCall}. */
  @FunctionalInterface
  public interface RexCallTransformer
      extends BiFunction<Context, RexCall, RexNode> {
  }

  /** Transforms a {@link RexCall}, how its caller prefers to receive NULL
   * values. */
  @FunctionalInterface
  public interface RexWeakCallTransformer {
    RexNode apply(Context context, RexCall call, RexUnknownAs unknownAs);
  }
}
