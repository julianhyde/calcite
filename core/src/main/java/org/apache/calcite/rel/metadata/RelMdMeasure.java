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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

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
      // Does not loop. We assume that every RelNode can expand.
      return mq.expand(rel, column, context);
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
    final RexCall call = (RexCall) e;
    final RexSubQuery scalarQuery =
        context.getRelBuilder().scalarQuery(b ->
            b.push(project.getInput())
                .project(call.operands.get(0))
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

}
