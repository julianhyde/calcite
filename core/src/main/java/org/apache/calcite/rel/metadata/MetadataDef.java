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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Definition of metadata.
 *
 * @param <M> Kind of metadata
 */
public class MetadataDef<M extends Metadata> {
  public final Class<M> metadataClass;
  public final Class<? extends MetadataHandler<M>> handlerClass;
  public final ImmutableList<Method> methods;

  private MetadataDef(Class<M> metadataClass,
      Class<? extends MetadataHandler<M>> handlerClass, Method... methods) {
    this.metadataClass = metadataClass;
    this.handlerClass = handlerClass;
    this.methods = ImmutableList.copyOf(methods);
    final Map<String, Method> handlerMethods =
        byName(handlerClass.getDeclaredMethods());

    // Handler must have the same methods as Metadata, each method having
    // additional "subclass-of-RelNode, RelMetadataQuery" parameters.
    for (Method method : methods) {
      final List<Class<?>> leftTypes =
          Arrays.asList(method.getParameterTypes());
      final Method handlerMethod = handlerMethods.get(method.getName());
      assert  handlerMethod != null;
      final List<Class<?>> rightTypes =
          Arrays.asList(handlerMethod.getParameterTypes());
      assert leftTypes.size() + 2 == rightTypes.size();
      assert RelNode.class.isAssignableFrom(rightTypes.get(0));
      assert RelMetadataQuery.class == rightTypes.get(1);
      assert leftTypes.equals(rightTypes.subList(2, rightTypes.size()));
    }
  }

  private Map<String, Method> byName(Method[] methods) {
    final ImmutableMap.Builder<String, Method> builder = ImmutableMap.builder();
    for (Method method : methods) {
      builder.put(method.getName(), method);
    }
    return builder.build();
  }

  /** Creates a {@link org.apache.calcite.rel.metadata.MetadataDef}. */
  public static <M extends Metadata> MetadataDef<M> of(Class<M> metadataClass,
      Class<? extends MetadataHandler<M>> handlerClass, Method... methods) {
    return new MetadataDef<>(metadataClass, handlerClass, methods);
  }
}
