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

import org.apache.calcite.rel.RelNode;

/**
 * Default implementations of the
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Measure}
 * metadata provider for the standard logical algebra.
 *
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery#isMeasure
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
}
