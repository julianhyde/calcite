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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;

/** Implementation of {@link org.apache.calcite.rel.core.Filter} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableFilter
    extends Filter
    implements EnumerableRel {
  /** Factory. */
  public static final EnumerableFilterFactory FACTORY =
      new EnumerableFilterFactory();

  /** Creates an EnumerableFilter.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() instanceof EnumerableConvention;
  }

  /** Creates an EnumerableFilter. */
  public static EnumerableFilter create(final RelNode input,
      RexNode condition) {
    return FACTORY.createFilter(input, condition);
  }

  public EnumerableFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new EnumerableFilter(getCluster(), traitSet, input, condition);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // EnumerableCalc is always better
    throw new UnsupportedOperationException();
  }

  /** Implementation of
   * {@link org.apache.calcite.rel.core.RelFactories.FilterFactory}
   * that returns an {@link EnumerableFilter}. */
  private static class EnumerableFilterFactory
      extends RelFactories.FilterFactoryImpl {
    @Override protected RelTraitSet traits(RelNode input) {
      return super.traits(input)
          .replace(EnumerableConvention.INSTANCE);
    }

    public EnumerableFilter createFilter(RelNode input, RexNode condition) {
      return new EnumerableFilter(input.getCluster(), traits(input), input,
          condition);
    }
  }
}

// End EnumerableFilter.java
