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

import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rex.RexRule.Done;
import org.apache.calcite.rex.RexRule.OperandBuilder;
import org.apache.calcite.rex.RexRule.OperandDetailBuilder;
import org.apache.calcite.rex.RexRule.OperandTransform;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A collection of {@link RexRule} instances that transforms a row-expression.
 *
 * <p>The analog for relational expressions is {@link HepProgram}.
 *
 * <p>A {@code RexRuleProgram} is immutable (as are its constituent rules). It
 * makes sense to create a program once and use it for many statement
 * preparations.
 *
 * <p>The program indexes its constituent rules so that, given an expression,
 * we can quickly determine whether any of the rules are applicable.
 */
public class RexRuleProgram {
  private final ImmutableMap<RexRule, Operand> ruleOperands;

  /** Private constructor.  */
  private RexRuleProgram(ImmutableMap<RexRule, Operand> ruleOperands) {
    this.ruleOperands = ruleOperands;
  }

  /** Creates a program. */
  public static RexRuleProgram of(Iterable<? extends RexRule> rules) {
    final ImmutableMap.Builder<RexRule, Operand> b = ImmutableMap.builder();
    rules.forEach(r -> b.put(r, toOperand(r)));
    return new RexRuleProgram(b.build());
  }

  private static Operand toOperand(RexRule rule) {
    final List<Operand> operands = new ArrayList<>();
    final OperandBuilderImpl operandBuilder = new OperandBuilderImpl(operands);
    rule.describe(operandBuilder);
    return operands.get(0);
  }

  /** Applies the program to an expression. */
  public RexNode apply(RexRule.Context cx, RexNode e) {
    return e.accept(
        new RexShuttle() {
          // TODO: more methods need to be overloaded

          @Override public RexNode visitLiteral(RexLiteral literal) {
            return applyRules(cx, literal);
          }

          @Override public RexNode visitCall(RexCall call) {
            return applyRules(cx, call);
          }
        });
  }

  /** Applies all rules to an expression. */
  private RexNode applyRules(RexRule.Context cx, RexNode e) {
    for (Map.Entry<RexRule, Operand> entry : ruleOperands.entrySet()) {
      final RexRule rule = entry.getKey();
      final Operand operand = entry.getValue();
      if (operand.matches(e)) {
        e = rule.apply(cx, e);
      }
    }
    return e;
  }

  /** Built by {@link OperandBuilder}. */
  private abstract static class Operand {
    abstract boolean matches(RexNode e);
  }

  /** Operand that may have children.
   *
   * <p>To match, the expression must match this operand
   * and the expression's operands must match the child operands. */
  abstract static class ParentOperand extends Operand {
    final List<Operand> operands;

    ParentOperand(List<Operand> operands) {
      this.operands = ImmutableList.copyOf(operands);
    }

    @Override boolean matches(RexNode e) {
      if (e instanceof RexCall) {
        final List<RexNode> operands = ((RexCall) e).operands;
        if (operands.size() != this.operands.size()) {
          return false;
        }
        for (Pair<Operand, RexNode> pair : Pair.zip(this.operands, operands)) {
          if (!pair.left.matches(pair.right)) {
            return false;
          }
        }
        return true;
      } else {
        return operands.isEmpty();
      }
    }
  }

  /** Operand that matches nodes of a given {@link SqlKind}. */
  static class KindOperand extends ParentOperand {
    final SqlKind kind;

    KindOperand(SqlKind kind, List<Operand> operands) {
      super(operands);
      this.kind = kind;
    }

    @Override boolean matches(RexNode e) {
      return e.getKind() == kind
          && super.matches(e);
    }
  }

  /** Implementation of {@link OperandBuilder}. */
  private static class OperandBuilderImpl implements OperandBuilder {
    final List<Operand> operands;

    OperandBuilderImpl(List<Operand> operands) {
      this.operands = operands;
    }

    private Done resultIs(Operand operand) {
      operands.add(operand);
      return DoneImpl.INSTANCE;
    }

    @Override public Done any() {
      return resultIs(new AnyOperand());
    }

    @Override public Done notNull() {
      return resultIs(new NotNullOperand());
    }

    @Override public Done isLiteral(Predicate<RexLiteral> predicate) {
      return resultIs(new LiteralOperand(predicate));
    }

    @Override public OperandDetailBuilder ofKind(SqlKind kind) {
      return new OperandDetailBuilderImpl(operands ->
          resultIs(new KindOperand(kind, operands)));
    }

    @Override public OperandDetailBuilder callTo(SqlOperator operator) {
      throw new AssertionError("TODO");
    }

    @Override public OperandBuilder callTo(
        Class<? extends SqlOperator> operatorClass) {
      throw new AssertionError("TODO");
    }

    /** Implementation of {@link OperandDetailBuilder}. */
    private static class OperandDetailBuilderImpl
        implements OperandDetailBuilder {
      final List<Operand> operands = new ArrayList<>();
      final Function<List<Operand>, Done> completer;

      OperandDetailBuilderImpl(Function<List<Operand>, Done> completer) {
        this.completer = completer;
      }

      @Override public OperandDetailBuilder predicate(
          Predicate<? super RexCall> predicate) {
        throw new AssertionError("TODO");
      }

      @Override public Done oneInput(OperandTransform transform) {
        return inputs(transform);
      }

      @Override public Done inputs(OperandTransform... transforms) {
        final OperandBuilderImpl operandBuilder = new OperandBuilderImpl(operands);
        for (OperandTransform transform : transforms) {
          transform.apply(operandBuilder);
        }
        return finish();
      }

      protected Done finish() {
        return completer.apply(operands);
      }

      @Override public Done anyInputs() {
        throw new AssertionError("TODO");
      }
    }
  }

  /** Operand that matches any expression that is NOT NULL. */
  private static class NotNullOperand extends Operand {
    @Override boolean matches(RexNode e) {
      // TODO: also allow operands that are NOT NULL due to predicates
      return !e.getType().isNullable();
    }
  }

  /** Operand that matches a {@link RexLiteral}, optionally with an extra
   * predicate. */
  private static class LiteralOperand extends Operand {
    private final Predicate<RexLiteral> predicate;

    LiteralOperand(Predicate<RexLiteral> predicate) {
      this.predicate = predicate;
    }

    @Override boolean matches(RexNode e) {
      return e instanceof RexLiteral
          && predicate.test((RexLiteral) e);
    }
  }

  /** Operand that matches any expression. */
  private static class AnyOperand extends Operand {
    @Override boolean matches(RexNode e) {
      return true;
    }
  }

  /** Singleton instance of {@link Done}. */
  private enum DoneImpl implements Done {
    INSTANCE
  }
}
