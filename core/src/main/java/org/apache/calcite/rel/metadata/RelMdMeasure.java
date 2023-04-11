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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Default implementations of the
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Measure}
 * metadata provider for the standard logical algebra.
 *
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery#isMeasure
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery#expand
 */
public class RelMdMeasure
    implements MetadataHandler<BuiltInMetadata.Measure> {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(new RelMdMeasure(), BuiltInMetadata.Measure.Handler.class);

  @Override public MetadataDef<BuiltInMetadata.Measure> getDef() {
    return BuiltInMetadata.Measure.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Measure#isMeasure(int)},
   * invoked using reflection.
   */
  public Boolean isMeasure(RelNode rel, RelMetadataQuery mq, int column) {
    return false;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Measure#expand(int, BuiltInMetadata.Measure.Context)},
   * invoked using reflection.
   */
  public RexNode expand(RelNode rel, RelMetadataQuery mq, int column,
      BuiltInMetadata.Measure.Context context) {
    throw new UnsupportedOperationException("expand(" + rel + ", " + column
        + ", " + context);
  }

  /** Refines {@code expand} for {@link RelSubset}; called via reflection. */
  public RexNode expand(RelSubset subset, RelMetadataQuery mq, int column,
      BuiltInMetadata.Measure.Context context) {
    for (RelNode rel : subset.getRels()) {
      RexNode e = mq.expand(rel, column, context);
      if (e != null) {
        return e;
      }
    }
    return expand((RelNode) subset, mq, column, context);
  }

  /** Refines {@code expand} for {@link Project}; called via reflection. */
  public RexNode expand(Project project, RelMetadataQuery mq, int column,
      BuiltInMetadata.Measure.Context context) {
    final RexNode e = project.getProjects().get(column);
    if (e.getKind() != SqlKind.V2M) {
      throw new AssertionError(e);
    }
    final BuiltInMetadata.Measure.Context context2 =
        new BuiltInMetadata.Measure.Context() {
          @Override public RelBuilder getRelBuilder() {
            return context.getRelBuilder();
          }

          @Override public List<RexNode> getFilters(RelBuilder b) {
            List<RexNode> filters = context.getFilters(b);
            return new RexShuttle() {
              @Override public RexNode visitInputRef(RexInputRef inputRef) {
                return project.getProjects().get(inputRef.getIndex());
              }
            }.apply(filters);
          }

          @Override public int getDimensionCount() {
            return (int) project.getInput().getRowType().getFieldList().stream()
                .filter(f -> !f.getType().isMeasure()).count();
          }
        };
    final RexCall call = (RexCall) e;
    final int dimensionCount = context.getDimensionCount();
    final RexSubQuery scalarQuery =
        context.getRelBuilder().scalarQuery(b ->
            b.push(project.getInput().stripped())
                .filter(context2.getFilters(b))
                .aggregateRex(b.groupKey(), call.operands.get(0))
                .build());
    final RelDataType measureType =
        SqlTypeUtil.fromMeasure(context.getTypeFactory(), call.type);
    if (!measureType.isNullable()) {
      // If the measure is 'MEASURE<INTEGER NOT NULL>' the scalar query
      // '(SELECT SUM(x) FROM t WHERE a = context.a)' that implements it looks
      // nullable but isn't really.
      return context.getRexBuilder().makeNotNull(scalarQuery);
    }
    return scalarQuery;
  }

  /** Refines {@code expand} for {@link Aggregate}; called via reflection. */
  public @Nullable RexNode expand(Aggregate aggregate, RelMetadataQuery mq,
      int column, BuiltInMetadata.Measure.Context context) {
    final AggregateCall e =
        aggregate.getAggCallList().get(column - aggregate.getGroupCount());
    if (e.getAggregation().getKind() != SqlKind.AGG_M2M) {
      throw new AssertionError(e);
    }
    int arg = getOnlyElement(e.getArgList());
    return mq.expand(aggregate.getInput().stripped(), arg, context);
  }

}
