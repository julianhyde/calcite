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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.util.regex.Pattern;

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
  public interface Line {
    int fnr();
    Source source();
    boolean startsWith(String prefix);
    boolean endsWith(String suffix);
    boolean matches(String regex);
    String line();
  }

  /** Context for executing a Puffin program within a given file. */
  public static class Context {
    final PrintWriter out;
    final Source source;
    private final LoadingCache<String, Pattern> patternCache;

    /** Holds the current line. */
    String line = "";

    /** Holds the current line number in the file (starting from 1).
     *
     * <p>Corresponds to the Awk variable {@code FNR}, which stands for "file
     * number of records". */
    int fnr = 0;

    Context(PrintWriter out, Source source,
        LoadingCache<String, Pattern> patternCache) {
      this.out = requireNonNull(out, "out");
      this.source = requireNonNull(source, "source");
      this.patternCache = requireNonNull(patternCache, "patternCache");
    }

    public void println(String s) {
      out.println(s);
    }

    Pattern pattern(String regex) {
      return patternCache.getUnchecked(regex);
    }
  }

  /** Extension to {@link Context} that also implements {@link Line}.
   *
   * <p>We don't want clients to know that {@code Context} implements
   * {@code Line}, but neither do we want to create a new {@code Line} object
   * for every line in the file. Making this a subclass accomplishes both
   * goals. */
  static class ContextImpl extends Context implements Line {
    ContextImpl(PrintWriter out, Source source,
        LoadingCache<String, Pattern> patternCache) {
      super(out, source, patternCache);
    }

    @Override public int fnr() {
      return fnr;
    }

    @Override public Source source() {
      return source;
    }

    @Override public boolean startsWith(String prefix) {
      return line.startsWith(prefix);
    }

    @Override public boolean endsWith(String suffix) {
      return line.endsWith(suffix);
    }

    @Override public boolean matches(String regex) {
      return pattern(regex).matcher(line).matches();
    }

    @Override public String line() {
      return line;
    }
  }

  /** Implementation of {@link Program}. */
  private static class ProgramImpl implements Program {
    private final PairList<Predicate<Line>, Consumer<Line>> pairList;
    private final ImmutableList<Consumer<Context>> endList;
    @SuppressWarnings("Convert2MethodRef")
    private final LoadingCache<String, Pattern> patternCache =
        CacheBuilder.newBuilder()
            .build(CacheLoader.from(regex -> Pattern.compile(regex)));

    private ProgramImpl(PairList<Predicate<Line>, Consumer<Line>> pairList,
        ImmutableList<Consumer<Context>> endList) {
      this.pairList = pairList;
      this.endList = endList;
    }

    @Override public void execute(Source source, PrintWriter out) {
      try (Reader r = source.reader();
           BufferedReader br = new BufferedReader(r)) {
        final ContextImpl x = new ContextImpl(out, source, patternCache);
        for (;;) {
          String lineText = br.readLine();
          if (lineText == null) {
            endList.forEach(end -> end.accept(x));
            break;
          }
          ++x.fnr;
          x.line = lineText;
          pairList.forEach((predicate, action) -> {
            if (predicate.test(x)) {
              action.accept(x);
            }
          });
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
