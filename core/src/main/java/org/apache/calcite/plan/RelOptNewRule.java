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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * Rule that is parameterized via a configuration.
 *
 * <p>Temporary.
 *
 * @param <C> Configuration type
 */
public abstract class RelOptNewRule<C extends RelOptNewRule.Config>
    extends RelOptRule {
  public final C config;

  public RelOptNewRule(C config) {
    super(OperandBuilderImpl.operand(config.operandSupplier()),
        config.relBuilderFactory(), config.description());
    this.config = config;
  }

  /** Rule configuration. */
  public interface Config {
    /** Empty configuration. */
    RelOptNewRule.Config EMPTY = ImmutableBeans.create(Config.class)
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .withOperandSupplier(b -> {
          throw new IllegalArgumentException("Rules must have at least one "
              + "operand. Call Config.withOperandSupplier to specify them.");
        });

    /** Creates a rule that uses this configuration. Sub-class must override. */
    RelOptRule toRule();

    /** Casts this configuration to another type, usually a sub-class. */
    default <T> T as(Class<T> class_) {
      return ImmutableBeans.copy(class_, this);
    }

    /** The factory that is used to create a
     * {@link org.apache.calcite.tools.RelBuilder} during rule invocations. */
    @ImmutableBeans.Property
    RelBuilderFactory relBuilderFactory();

    /** Sets {@link #relBuilderFactory()}. */
    Config withRelBuilderFactory(RelBuilderFactory factory);

    /** Description of the rule instance. */
    @ImmutableBeans.Property
    String description();

    /** Sets {@link #description()}. */
    Config withDescription(String description);

    /** Creates the operands for the rule instance. */
    @ImmutableBeans.Property
    OperandTransform operandSupplier();

    /** Sets {@link #operandSupplier()}. */
    Config withOperandSupplier(OperandTransform transform);
  }

  /** Function that creates an operand. */
  @FunctionalInterface
  public interface OperandTransform extends Function<OperandBuilder, Done> {
  }

  /** Callback to create an operand. */
  public interface OperandBuilder {
    /** Starts building an operand by specifying its class.
     * Call further methods on the returned {@link OperandDetailBuilder} to
     * complete the operand. */
    <R extends RelNode> OperandDetailBuilder<R> operand(Class<R> relClass);

    /** Supplies an operand that has been built manually. */
    Done exactly(RelOptRuleOperand operand);
  }

  /** Indicates that an operand is complete. */
  public interface Done {
  }

  /** Add details about an operand, such as its inputs.
   *
   * @param <R> Type of relational expression */
  public interface OperandDetailBuilder<R extends RelNode> {
    /** Sets a trait of this operand. */
    OperandDetailBuilder<R> trait(@Nonnull RelTrait trait);

    /** Sets the predicate of this operand. */
    OperandDetailBuilder<R> predicate(Predicate<? super R> predicate);

    /** Indicates that this operand has a single input. */
    Done oneInput(OperandTransform transform);

    /** Indicates that this operand has several inputs. */
    Done inputs(OperandTransform... transforms);

    /** Indicates that this operand has several inputs, unordered. */
    Done unorderedInputs(OperandTransform... transforms);

    /** Indicates that this operand takes any number or type of inputs. */
    Done anyInputs();

    /** Indicates that this operand takes no inputs. */
    Done noInputs();

    /** Indicates that this operand converts a relational expression to
     * another trait. */
    Done convert(RelTrait in);
  }

  /** Implementation of {@link OperandBuilder}. */
  private static class OperandBuilderImpl implements OperandBuilder {
    final List<RelOptRuleOperand> operands = new ArrayList<>();

    static RelOptRuleOperand operand(OperandTransform transform) {
      final OperandBuilderImpl b = new OperandBuilderImpl();
      final Done done = transform.apply(b);
      Objects.requireNonNull(done);
      if (b.operands.size() != 1) {
        throw new IllegalArgumentException("operand supplier must call one of "
            + "the following methods: operand or exactly");
      }
      return b.operands.get(0);
    }

    public <R extends RelNode> OperandDetailBuilder<R> operand(Class<R> relClass) {
      return new OperandDetailBuilderImpl<>(this, relClass);
    }

    public Done exactly(RelOptRuleOperand operand) {
      operands.add(operand);
      return DoneImpl.INSTANCE;
    }
  }

  /** Implementation of {@link OperandDetailBuilder}.
   *
   * @param <R> Type of relational expression */
  private static class OperandDetailBuilderImpl<R extends RelNode>
      implements OperandDetailBuilder<R> {
    private final OperandBuilderImpl parent;
    private final Class<R> relClass;
    final OperandBuilderImpl inputBuilder = new OperandBuilderImpl();
    private RelTrait trait;
    private Predicate<? super R> predicate = r -> true;

    OperandDetailBuilderImpl(OperandBuilderImpl parent, Class<R> relClass) {
      this.parent = Objects.requireNonNull(parent);
      this.relClass = Objects.requireNonNull(relClass);
    }

    public OperandDetailBuilderImpl<R> trait(@Nonnull RelTrait trait) {
      this.trait = Objects.requireNonNull(trait);
      return this;
    }

    public OperandDetailBuilderImpl<R> predicate(Predicate<? super R> predicate) {
      this.predicate = predicate;
      return this;
    }

    /** Indicates that there are no more inputs. */
    Done done(RelOptRuleOperandChildPolicy childPolicy) {
      parent.operands.add(
          new RelOptRuleOperand(relClass, trait, predicate, childPolicy,
              ImmutableList.copyOf(inputBuilder.operands)));
      return DoneImpl.INSTANCE;
    }

    public Done convert(RelTrait in) {
      parent.operands.add(
          new ConverterRelOptRuleOperand(relClass, in, predicate));
      return DoneImpl.INSTANCE;
    }

    public Done noInputs() {
      return done(RelOptRuleOperandChildPolicy.LEAF);
    }

    public Done anyInputs() {
      return done(RelOptRuleOperandChildPolicy.ANY);
    }

    public Done oneInput(OperandTransform transform) {
      final Done done = transform.apply(inputBuilder);
      Objects.requireNonNull(done);
      return done(RelOptRuleOperandChildPolicy.SOME);
    }

    public Done inputs(OperandTransform... transforms) {
      for (OperandTransform transform : transforms) {
        final Done done = transform.apply(inputBuilder);
        Objects.requireNonNull(done);
      }
      return done(RelOptRuleOperandChildPolicy.SOME);
    }

    public Done unorderedInputs(OperandTransform... transforms) {
      for (OperandTransform transform : transforms) {
        final Done done = transform.apply(inputBuilder);
        Objects.requireNonNull(done);
      }
      return done(RelOptRuleOperandChildPolicy.UNORDERED);
    }
  }

  /** Singleton instance of {@link Done}. */
  private enum DoneImpl implements Done {
    INSTANCE
  }

  /** Callback interface that helps you avoid creating sub-classes of
   * {@link RelOptNewRule} that differ only in implementations of
   * {@link #onMatch(RelOptRuleCall)} method.
   *
   * @param <R> Rule type */
  public interface MatchHandler<R extends RelOptRule>
      extends BiConsumer<R, RelOptRuleCall> {
  }
}
