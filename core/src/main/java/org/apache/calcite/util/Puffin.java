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

import org.apache.calcite.runtime.PairList;

import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * A text processor similar to Awk.
 *
 * <p>Example use:
 *
 * <blockquote><pre>{@code
 * File file;
 * final Puffin.Program program =
 *   Puffin.builder()
 *       .add(line -> !line.startsWith("#"),
 *           line -> counter.incrementAndGet())
 *       .after(context ->
 *           context.println("There were " + counter.get()
 *               + " uncommented lines"))
 *       .build();
 * program.execute(Source.of(file), System.out);
 * }</pre></blockquote>
 *
 * <p>prints the following to stdout:
 *
 * <blockquote>{@code
 * There were 3 uncommented lines.
 * }</blockquote>
 */
public class Puffin {
  private Puffin() {
  }

  public static Builder builder() {
    return new Builder() {
      final PairList<Predicate<Line>, Consumer<Line>> mutablePairList =
          PairList.of();
      final List<Consumer<Context>> afterList = new ArrayList<>();

      @Override public Builder add(Predicate<Line> linePredicate,
          Consumer<Line> action) {
        mutablePairList.add(linePredicate, action);
        return this;
      }

      @Override public Builder after(Consumer<Context> action) {
        afterList.add(action);
        return this;
      }

      @Override public Program build() {
        return new ProgramImpl(mutablePairList.immutable(),
            ImmutableList.copyOf(afterList));
      }
    };
  }

  /** Fluent interface for constructing a Program.
   *
   * @see Puffin#builder() */
  public interface Builder {
    Builder add(Predicate<Line> linePredicate,
        Consumer<Line> action);

    Builder after(Consumer<Context> action);
    Program build();

  }

  /** A Puffin program. You can execute it on a file. */
  public interface Program {
    /** Executes this program. */
    void execute(Source source, PrintWriter out);

    /** Executes this program, writing to an output stream such as
     * {@link System#out}. */
    default void execute(Source source, OutputStream out) {
      try (PrintWriter w = Util.printWriter(out)) {
        execute(source, w);
      }
    }
  }

  /** A line in a file.
   *
   * <p>Created by an executing program and passed to the predicate
   * and action that you registered in
   * {@link Builder#add(Predicate, Consumer)}.
   */
  public static class Line {
    final Context context;
    public final String line;

    Line(Context context, String line) {
      this.context = context;
      this.line = line;
    }

    public int fnr() {
      return context.fnr[0];
    }

    public Source source() {
      return context.source;
    }

    public boolean startsWith(String prefix) {
      return line.startsWith(prefix);
    }
  }

  /** Context for executing a Puffin program within a given file. */
  public static class Context {
    final PrintWriter out;
    final Source source;

    /** Holds the current line number in the file (starting from 1).
     *
     * <p>Corresponds to the Awk variable {@code FNR}, which stands for "file
     * number of records". */
    final int[] fnr = {0};

    Context(PrintWriter out, Source source) {
      this.out = requireNonNull(out, "out");
      this.source = requireNonNull(source, "source");
    }

    public void println(String s) {
      out.println(s);
    }
  }

  /** Implementation of {@link Program}. */
  private static class ProgramImpl implements Program {
    private final PairList<Predicate<Line>, Consumer<Line>> pairList;
    private final ImmutableList<Consumer<Context>> endList;

    private ProgramImpl(PairList<Predicate<Line>, Consumer<Line>> pairList,
        ImmutableList<Consumer<Context>> endList) {
      this.pairList = pairList;
      this.endList = endList;
    }

    @Override public void execute(Source source, PrintWriter out) {
      try (Reader r = source.reader();
           BufferedReader br = new BufferedReader(r)) {
        Context x = new Context(out, source);
        for (;;) {
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
    }
  }
}
