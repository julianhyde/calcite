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
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;

import com.google.common.base.Suppliers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Planner rule that removes keys from
 * a {@link Exchange} if those keys are known to be constant.
 *
 * <p>For example,
 * <code>SELECT key,value FROM (SELECT 1 AS key, value FROM src) r DISTRIBUTE
 * BY key</code> can be reduced to
 * <code>SELECT 1 AS key, value FROM src</code>.</p>
 *
 * @see SortExchangeRemoveConstantKeysRule
 */
public class ExchangeRemoveConstantKeysRule extends RelOptNewRule
    implements SubstitutionRule {
  /**
   * Singleton rule that removes constants inside a
   * {@link LogicalExchange}.
   */
  public static final ExchangeRemoveConstantKeysRule EXCHANGE_INSTANCE =
      Config.EMPTY
          .as(Config.class)
          .withOperandFor(LogicalExchange.class)
          .toRule();

  /** @deprecated Use {@link SortExchangeRemoveConstantKeysRule#INSTANCE}. */
  @Deprecated
  public static final Supplier<SortExchangeRemoveConstantKeysRule>
      SORT_EXCHANGE_INSTANCE =
      Suppliers.memoize(() -> SortExchangeRemoveConstantKeysRule.INSTANCE)::get;

  /** Creates an ExchangeRemoveConstantKeysRule. */
  protected ExchangeRemoveConstantKeysRule(Config config) {
    super(config);
  }

  /** Removes constant in distribution keys. */
  protected static List<Integer> simplifyDistributionKeys(RelDistribution distribution,
      Set<Integer> constants) {
    return distribution.getKeys().stream()
        .filter(key -> !constants.contains(key))
        .collect(Collectors.toList());
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Exchange exchange = call.rel(0);
    return exchange.getDistribution().getType()
        == RelDistribution.Type.HASH_DISTRIBUTED;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Exchange exchange = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelNode input = exchange.getInput();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (predicates == null) {
      return;
    }

    final Set<Integer> constants = new HashSet<>();
    predicates.constantMap.keySet().forEach(key -> {
      if (key instanceof RexInputRef) {
        constants.add(((RexInputRef) key).getIndex());
      }
    });
    if (constants.isEmpty()) {
      return;
    }

    final List<Integer> distributionKeys = simplifyDistributionKeys(
        exchange.getDistribution(), constants);

    if (distributionKeys.size() != exchange.getDistribution().getKeys()
        .size()) {
      call.transformTo(call.builder()
          .push(exchange.getInput())
          .exchange(distributionKeys.isEmpty()
              ? RelDistributions.SINGLETON
              : RelDistributions.hash(distributionKeys))
          .build());
      call.getPlanner().prune(exchange);
    }
  }

  /** Rule configuration. */
  public interface Config extends RelOptNewRule.Config {
    @Override default ExchangeRemoveConstantKeysRule toRule() {
      return new ExchangeRemoveConstantKeysRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Exchange> exchangeClass) {
      return withOperandSupplier(b -> b.operand(exchangeClass).anyInputs())
          .as(Config.class);
    }
  }
}
