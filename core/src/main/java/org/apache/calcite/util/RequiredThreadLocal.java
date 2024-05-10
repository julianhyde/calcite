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
package org.apache.calcite.util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Thread-local variable whose value can never be null.
 *
 * <p>The implementation ensures checks that the supplier never supplies null,
 * and that {@link #set} is never called with null; therefore the caller is
 * guaranteed that {@link #get} never returns null.
 *
 * @param <T> Value type
 */
public abstract class RequiredThreadLocal<T> extends ThreadLocal<@Nullable T> {
  /** Prevent implementations outside of this class file. */
  private RequiredThreadLocal() {
    super();
  }

  /** Creates a RequiredThreadLocal.
   *
   * @param supplier Supplier
   */
  public static <S> RequiredThreadLocal<S> withInitial(
      Supplier<? extends @NonNull S> supplier) {
    return new SuppliedThreadLocal<>(supplier);
  }

  @Override public @NonNull T get() {
    // Null value should never happen, because supplier never returns null,
    // and 'set' does not allow null value.
    return requireNonNull(super.get());
  }

  @Override public void set(@Nullable T value) {
    super.set(requireNonNull(value, "value must not be null"));
  }

  @Override protected abstract @NonNull T initialValue();

  /** Implementation of {@link org.apache.calcite.util.RequiredThreadLocal}
   * that has a supplier for initial values.
   *
   * @param <T> Value type
   */
  private static final class SuppliedThreadLocal<T>
      extends RequiredThreadLocal<T> {
    private final Supplier<? extends @NonNull T> supplier;

    SuppliedThreadLocal(Supplier<? extends @NonNull T> supplier) {
      this.supplier = requireNonNull(supplier, "supplier");
    }

    @Override protected @NonNull T initialValue() {
      return requireNonNull(supplier.get(), "supplier returned null");
    }
  }
}
