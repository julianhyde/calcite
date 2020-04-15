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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptNewRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Union}
 * (<code>all</code> = <code>false</code>)
 * into an {@link org.apache.calcite.rel.core.Aggregate}
 * on top of a non-distinct {@link org.apache.calcite.rel.core.Union}
 * (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule
    extends RelOptNewRule<UnionToDistinctRule.Config>
    implements TransformationRule {
  public static final UnionToDistinctRule INSTANCE = Config.EMPTY
      .as(Config.class)
      .withOperandFor(LogicalUnion.class)
      .toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a UnionToDistinctRule. */
  protected UnionToDistinctRule(Config config) {
    super(config);
  }

  @Deprecated
  public UnionToDistinctRule(Class<? extends Union> unionClass,
      RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config.withOperandFor(unionClass)
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public UnionToDistinctRule(Class<? extends Union> unionClazz,
      RelFactories.SetOpFactory setOpFactory) {
    this(INSTANCE.config.withOperandFor(unionClazz)
        .withRelBuilderFactory(RelBuilder.proto(setOpFactory))
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Union union = call.rel(0);
    final RelBuilder relBuilder = call.builder();
    relBuilder.pushAll(union.getInputs());
    relBuilder.union(true, union.getInputs().size());
    relBuilder.distinct();
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelOptNewRule.Config {
    @Override default UnionToDistinctRule toRule() {
      return new UnionToDistinctRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Union> unionClass) {
      return withOperandSupplier(b ->
          b.operand(unionClass)
              .predicate(union -> !union.all).anyInputs())
          .as(Config.class);
    }
  }
}
