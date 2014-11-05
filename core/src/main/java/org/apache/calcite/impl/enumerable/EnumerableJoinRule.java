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
package org.apache.calcite.impl.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.ArrayList;
import java.util.List;

/**
* Created by jhyde on 10/27/14.
*/
class EnumerableJoinRule extends ConverterRule {
  EnumerableJoinRule() {
    super(
        LogicalJoin.class,
        Convention.NONE,
        EnumerableConvention.INSTANCE,
        "EnumerableJoinRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    List<RelNode> newInputs = new ArrayList<RelNode>();
    for (RelNode input : join.getInputs()) {
      if (!(input.getConvention() instanceof EnumerableConvention)) {
        input =
            convert(
                input,
                input.getTraitSet()
                    .replace(EnumerableConvention.INSTANCE));
      }
      newInputs.add(input);
    }
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final JoinInfo info = JoinInfo.of(left, right, join.getCondition());
    if (!info.isEqui() && join.getJoinType() != JoinRelType.INNER) {
      // EnumerableJoinRel only supports equi-join. We can put a filter on top
      // if it is an inner join.
      return null;
    }
    final RelOptCluster cluster = join.getCluster();
    RelNode newRel;
    try {
      newRel = new EnumerableJoin(
          cluster,
          join.getTraitSet().replace(EnumerableConvention.INSTANCE),
          left,
          right,
          info.getEquiCondition(left, right, cluster.getRexBuilder()),
          info.leftKeys,
          info.rightKeys,
          join.getJoinType(),
          join.getVariablesStopped());
    } catch (InvalidRelException e) {
      EnumerableRules.LOGGER.fine(e.toString());
      return null;
    }
    if (!info.isEqui()) {
      newRel = new EnumerableFilter(cluster, newRel.getTraitSet(),
          newRel, info.getRemaining(cluster.getRexBuilder()));
    }
    return newRel;
  }
}

// End EnumerableJoinRule.java
