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
package org.apache.calcite.piglet;

import org.apache.pig.builtin.BigDecimalMax;
import org.apache.pig.builtin.BigDecimalSum;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.data.Tuple;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;

/**
 * The current Calcite enumerable engine does not correctly generate
 * Java code that can handle checked exceptions. This is temporary
 * solution that catches the checked exceptions early so that the
 * enumerable code generator does not have to handle them.
 *
 * <p>Note that without this wrapper, we still correctly convert Pig
 * to relational algebra, but we just cannot run the converted plans
 * with the Calcite enumerable engine.
 *
 * <p>TODO: [CALCITE-3195] Fix the enumerable code generator, then remove this
 * class.
 */
public class PigUdfWrapper {
  private PigUdfWrapper() {}

  public static boolean useUdfWrapper = false;

  public static final Map<String, Method> UDF_WRAPPER;
  static {
    final ImmutableMap.Builder<String, Method> map = ImmutableMap.builder();
    for (Method method : PigUdfWrapper.class.getDeclaredMethods()) {
      map.put(method.getName(), method);
    }
    UDF_WRAPPER = map.build();
  }

  public static Method getWrappedMethod(String simpleClassName) {
    if (useUdfWrapper) {
      final String methodName = simpleClassName.toLowerCase(Locale.ROOT);
      return UDF_WRAPPER.get(methodName);
    }
    return null;
  }

  public static Long count(Tuple input) {
    try {
      return new COUNT().exec(input);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected IOException from Pig UDF. Exception: " + e.getMessage());
    }
  }

  public static BigDecimal bigdecimalsum(Tuple input) {
    try {
      return new BigDecimalSum().exec(input);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected IOException from Pig UDF. Exception: " + e.getMessage());
    }
  }

  public static BigDecimal bigdecimalmax(Tuple input) {
    try {
      return new BigDecimalMax().exec(input);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected IOException from Pig UDF. Exception: " + e.getMessage());
    }
  }

}

// End PigUdfWrapper.java
