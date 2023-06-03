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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.runtime.PairList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A text processor similar to Awk.
 */
public class Puffin {
  public static Builder builder() {
    final PairList<Predicate<Line>, Consumer<Line>> pairList = PairList.of();

    return new Builder() {
      @Override public Builder add(Predicate<Line> linePredicate,
          Consumer<Line> action) {
        pairList.add(linePredicate, action);
        return this;
      }

      @Override public Program build() {
        final ImmutableList<Consumer<Context>> endList = ImmutableList.of();
        return (file, out) -> {
          try (FileReader r = new FileReader(file);
               BufferedReader br = new BufferedReader(r)) {
            Context x = new Context(out, file.getName());
            for (; ; ) {
              String line = br.readLine();
              if (line == null) {
                endList.forEach(end -> end.accept(x));
                break;
              }
              ++x.fnr[0];
              final Line line1 = new Line(x, line);
              pairList.forEach((predicate, action) -> {
                if (predicate.test(line1)) {
                  action.accept(line1);
                }
              });
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
      }
    };
  }

  /** Builds a Program. */
  public interface Builder {
    Builder add(Predicate<Line> linePredicate,
        Consumer<Line> action);

    Program build();
  }

  /** A Puffin program. You can execute it on a file. */
  public interface Program {
    void execute(File file, PrintWriter out);
  }

  /** A line in a file.
   *
   * <p>Created by an executing program and passed to the predicate
   * and action that you registered in
   * {@link Builder#add(Predicate, Consumer)}.</p>
   */
  public static class Line {
    final Context context;
    public final String line;

    Line(Context context, String line) {
      this.context = context;
      this.line = line;
    }

    public String filename() {
      return context.filename;
    }

    public int fnr() {
      return context.fnr[0];
    }
  }

  /** Context for executing a Puffin program within a given file. */
  static class Context {
    final PrintWriter out;
    final String filename;
    final int[] fnr = {0};

    Context(PrintWriter out, String filename) {
      this.out = out;
      this.filename = filename;
    }
  }
}
