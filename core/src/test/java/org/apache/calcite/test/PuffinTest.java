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
package org.apache.calcite.test;

import org.apache.calcite.runtime.Unit;
import org.apache.calcite.util.Puffin;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/** Tests {@link Puffin}. */
public class PuffinTest {
  private static final Fixture<Unit> EMPTY_FIXTURE =
      new Fixture<>(Sources.of(""), Puffin.builder().build());

  @Test void testPuffin() {
    Puffin.Program<Unit> program =
        Puffin.builder(() -> Unit.INSTANCE, u -> new AtomicInteger())
            .add(line -> !line.startsWith("#")
                    && !line.matches(".*/\\*.*\\*/.*"),
                line -> line.state().incrementAndGet())
            .after(context ->
                context.println("counter: " + context.state().get()))
            .build();
    fixture().withDefaultInput()
        .withProgram(program)
        .generatesOutput(isLinux("counter: 2\n"));
  }

  @Test void testEmptyProgram() {
    final Puffin.Program<Unit> program = Puffin.builder().build();
    fixture().withDefaultInput()
        .withProgram(program)
        .generatesOutput(is(""));
  }

  static Fixture<Unit> fixture() {
    return EMPTY_FIXTURE;
  }

  /** Fixture that contains all the state necessary to test
   * {@link Puffin}.
   *
   * @param <G> Type of state that is created when we start processing */
  private static class Fixture<G> {
    private final Source source;
    private final Puffin.Program<G> program;

    Fixture(Source source, Puffin.Program<G> program) {
      this.source = source;
      this.program = program;
    }

    public Fixture<G> withDefaultInput() {
      final String inputText = "first line\n"
          + "# second line\n"
          + "third line /* with a comment */\n"
          + "fourth line";
      return withSource(Sources.of(inputText));
    }

    private Fixture<G> withSource(Source source) {
      return new Fixture<>(source, program);
    }

    public <G2> Fixture<G2> withProgram(Puffin.Program<G2> program) {
      return new Fixture<>(source, program);
    }

    public Fixture<G> generatesOutput(Matcher<String> matcher) {
      StringWriter sw = new StringWriter();
      try (PrintWriter pw = new PrintWriter(sw)) {
        G g = program.execute(Stream.of(source), pw);
        assertThat(g, notNullValue());
      }
      assertThat(sw, hasToString(matcher));
      return this;
    }
  }
}
