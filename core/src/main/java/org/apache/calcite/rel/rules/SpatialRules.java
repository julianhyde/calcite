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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Predicate;

/**
 * Collection of planner rules that convert
 * calls to spatial functions into more efficient expressions.
 *
 * <p>The rules allow Calcite to use spatial indexes. For example is rewritten
 *
 * <blockquote>SELECT ...
 * FROM Restaurants AS r
 * WHERE ST_DWithin(ST_Point(10, 20), ST_Point(r.latitude, r.longitude), 5)
 * </blockquote>
 *
 * <p>to
 *
 * <blockquote>SELECT ...
 * FROM Restaurants AS r
 * WHERE (r.h BETWEEN 100 AND 150
 *        OR r.h BETWEEN 170 AND 185)
 * AND ST_DWithin(ST_Point(10, 20), ST_Point(r.latitude, r.longitude), 5)
 * </blockquote>
 *
 * <p>if there is the constraint
 *
 * <blockquote>CHECK (h = Hilbert(8, r.latitude, r.longitude))</blockquote>
 *
 * <p>If the {@code Restaurants} table is sorted on {@code h} then the latter
 * query can be answered using two limited range-scans, and so is much more
 * efficient.
 *
 * <p>Note that the original predicate
 * {@code ST_DWithin(ST_Point(10, 20), ST_Point(r.latitude, r.longitude), 5)}
 * is still present, but is evaluated after the approximate predicate has
 * eliminated many potential matches.
 */
public abstract class SpatialRules {

  private SpatialRules() {}

  private static final Predicate<Filter> FILTER_PREDICATE =
      new PredicateImpl<Filter>() {
        public boolean test(Filter filter) {
          switch (filter.getCondition().getKind()) {
          case ST_DWITHIN:
            return true;
          }
          return false;
        }
      };

  public static final RelOptRule INSTANCE =
      new FilterHilbertRule(RelFactories.LOGICAL_BUILDER);


  /** Rule that converts ST_DWithin in a Filter condition into a predicate on
   * a Hilbert curve. */
  @SuppressWarnings("WeakerAccess")
  public static class FilterHilbertRule extends RelOptRule {
    public FilterHilbertRule(RelBuilderFactory relBuilderFactory) {
      super(operand(Filter.class, null, FILTER_PREDICATE, any()),
          relBuilderFactory, "FilterHilbertRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      RexNode condition = filter.getCondition();
      final RelBuilder relBuilder = call.builder();
      call.transformTo(relBuilder.build());
    }
  }
}

// End SpatialRules.java
