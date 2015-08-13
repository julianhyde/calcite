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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression that describes how the result of a query should be
 * presented.
 *
 * <p>It only appears at the root of the tree of relational expressions, hence
 * the name.
 *
 * <p>Aspects of presentation:
 * <ul>
 *   <li>Field names (duplicates allowed)</li>
 *   <li>Sort order (may reference fields that are not projected)</li>
 * </ul>
 */
public class Root extends SingleRel {
  private final ImmutableList<Pair<Integer, String>> fields;
  private final RelCollation collation;

  /**
   * Creates a Root.
   *
   * @param cluster  Cluster that this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param input    Input relational expression
   * @param fields    Field references and names
   * @param collation Fields of underlying relational expression sorted on
   */
  protected Root(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<Pair<Integer, String>> fields, RelCollation collation) {
    super(cluster, traitSet, input);
    this.fields = ImmutableList.copyOf(fields);
    this.collation = Preconditions.checkNotNull(collation);
  }

  /**
   * Creates a Root by parsing serialized output.
   */
  protected Root(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        Pair.zip(input.getIntegerList("fields"), input.getStringList("names")),
        RelCollationTraitDef.INSTANCE.canonize(input.getCollation()));
  }

  public static Root create(RelNode input, List<Pair<Integer, String>> fields,
      RelCollation collation) {
    return new Root(input.getCluster(), input.getTraitSet(), input,
        fields, collation);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  /**
   * Copies a Root.
   *
   * @param traitSet Traits
   * @param input Input
   *
   * @see #copy(RelTraitSet, List)
   */
  public Root copy(RelTraitSet traitSet, RelNode input) {
    return new Root(input.getCluster(), traitSet, input, fields, collation);
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeTinyCost();
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("fields", Pair.left(fields))
        .item("names", Pair.right(fields))
        .item("collation", collation);
  }
}

// End Root.java
