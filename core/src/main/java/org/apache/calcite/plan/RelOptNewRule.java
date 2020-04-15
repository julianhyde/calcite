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

import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import java.util.function.Supplier;

/**
 * Rule that is parameterized via a configuration.
 *
 * <p>Temporary.
 */
public abstract class RelOptNewRule extends RelOptRule {
  protected final Config config;

  public RelOptNewRule(Config config) {
    super(config.operandSupplier().get(), config.relBuilderFactory(), config.description());
    this.config = config;
  }

  public Config config() {
    return config;
  }

  /** Rule configuration. */
  public interface Config {
    /** Creates a rule that uses this configuration. Sub-class must override. */
    default RelOptRule toRule() {
      throw new UnsupportedOperationException();
    }

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
    @ImmutableBeans.Property

    /** Creates the operands for the rule instance. */
    Supplier<RelOptRuleOperand> operandSupplier();

    /** Sets {@link #operandSupplier()}. */
    Config withOperandSupplier(Supplier<RelOptRuleOperand> supplier);
  }
}
