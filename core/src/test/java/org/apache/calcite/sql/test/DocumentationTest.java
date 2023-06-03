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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlOverlapsOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.test.concurrent.ConcurrentTestCommandScript;
import org.apache.calcite.util.Puffin;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Various automated checks on the documentation. */
class DocumentationTest {
  /** Generates a copy of {@code reference.md} with the current set of key
   * words. Fails if the copy is different from the original. */
  @Test void testGenerateKeyWords() throws IOException {
    final FileFixture f = new FileFixture();
    f.outFile.getParentFile().mkdirs();
    try (BufferedReader r = Util.reader(f.inFile);
         FileOutputStream fos = new FileOutputStream(f.outFile);
         PrintWriter w = Util.printWriter(f.outFile)) {
      String line;
      int stage = 0;
      while ((line = r.readLine()) != null) {
        if (line.equals("{% comment %} end {% endcomment %}")) {
          ++stage;
        }
        if (stage != 1) {
          w.println(line);
        }
        if (line.equals("{% comment %} start {% endcomment %}")) {
          ++stage;
          SqlAbstractParserImpl.Metadata metadata =
              new SqlParserTest().fixture().parser().getMetadata();
          int z = 0;
          for (String s : metadata.getTokens()) {
            if (z++ > 0) {
              w.println(",");
            }
            if (metadata.isKeyword(s)) {
              w.print(metadata.isReservedWord(s) ? ("**" + s + "**") : s);
            }
          }
          w.println(".");
        }
      }
      w.flush();
      fos.flush();
      fos.getFD().sync();
    }
    String diff = DiffTestCase.diff(f.outFile, f.inFile);
    if (!diff.isEmpty()) {
      throw new AssertionError("Mismatch between " + f.outFile
          + " and " + f.inFile + ":\n"
          + diff);
    }
  }

  /** Tests that every function in {@link SqlStdOperatorTable} is documented in
   * reference.md. */
  @Test void testAllFunctionsAreDocumented() throws IOException {
    final FileFixture f = new FileFixture();
    final Map<String, PatternOp> map = new TreeMap<>();

    final SqlStdOperatorTable standard = SqlStdOperatorTable.instance();
    addOperators(map, "", standard.getOperatorList());

    for (SqlLibrary library : SqlLibrary.values()) {
      final SqlOperatorTable libraryTable =
          SqlLibraryOperatorTableFactory.INSTANCE
              .getOperatorTable(EnumSet.of(library), false);
      switch (library) {
      case STANDARD:
      case SPATIAL:
        continue;
      case ALL:
        addOperators(map, "\\| \\* ", libraryTable.getOperatorList());
        continue;
      default:
        addOperators(map, "\\| [^|]*" + library.abbrev + "[^|]* ",
            libraryTable.getOperatorList());
      }
    }
    final Set<String> regexSeen = new HashSet<>();
    try (LineNumberReader r = new LineNumberReader(Util.reader(f.inFile))) {
      for (;;) {
        final String line = r.readLine();
        if (line == null) {
          break;
        }
        for (Map.Entry<String, PatternOp> entry : map.entrySet()) {
          if (entry.getValue().pattern.matcher(line).matches()) {
            regexSeen.add(entry.getKey()); // function is documented
          }
        }
      }
    }
    final Set<String> regexNotSeen = new TreeSet<>(map.keySet());
    regexNotSeen.removeAll(regexSeen);
    assertThat("some functions are not documented: " + map.entrySet().stream()
            .filter(e -> regexNotSeen.contains(e.getKey()))
            .map(e -> e.getValue().opName + "(" + e.getKey() + ")")
            .collect(Collectors.joining(", ")),
        regexNotSeen.isEmpty(), is(true));
  }

  private void addOperators(Map<String, PatternOp> map, String prefix,
      List<SqlOperator> operatorList) {
    for (SqlOperator op : operatorList) {
      final String name = op.getName().equals("TRANSLATE3") ? "TRANSLATE"
          : op.getName();
      if (op instanceof SqlSpecialOperator
          || !name.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
        continue;
      }
      final String regex;
      if (op instanceof SqlOverlapsOperator) {
        regex = "[ ]*<td>period1 " + name + " period2</td>";
      } else if (op instanceof SqlFunction
          && (op.getOperandTypeChecker() == null
              || op.getOperandTypeChecker().getOperandCountRange().getMin()
                  != 0)) {
        regex = prefix + "\\| .*" + name + "\\(.*";
      } else {
        regex = prefix + "\\| .*" + name + ".*";
      }
      map.put(regex, new PatternOp(Pattern.compile(regex), name));
    }
  }

  /** Tests that source code has no flaws. */
  @Test void testLint() {
    /** Warning that code is not as it should be. */
    class Message {
      final Source source;
      final int line;
      final String message;

      Message(Source source, int line, String message) {
        this.source = source;
        this.line = line;
        this.message = message;
      }
    }

    /** Internal state of the lint rules. */
    class State {
      int fileCount = 0;
      List<Message> messages = new ArrayList<>();
      int starLine;
      int atLine;
      int javadocStartLine;
      int javadocEndLine;

      void message(String message, Puffin.Line line) {
        messages.add(new Message(line.source(), line.fnr(), message));
      }

      void message(Puffin.Line line) {
        message(line.line, line);
      }

    }
    final State state = new State();

    // A line that has only a Javadoc paragraph marker, like this:
    //   * <p>
    final Pattern pEndPattern = Pattern.compile("^ *\\* <p>");

    // A line that starts a Javadoc paragraph, like this:
    //   * <p>The start of a paragraph.
    final Pattern pPattern = Pattern.compile("^ *\\* <p>.*");

    // A line that starts a Javadoc annotation, like this:
    //   * @param Param
    //   * @deprecated
    final Pattern atPattern = Pattern.compile("^ *\\* @.*");

    // A line that consists of only a star, like this:
    //   *
    final Pattern starPattern = Pattern.compile("^ *\\*");

    // A line that starts a javadoc block, like this:
    //   /** The start of a javadoc block.
    //   /** A single line javadoc block. */
    final Pattern javadocStartPattern = Pattern.compile("^ */\\*\\*.*");

    // A line that ends a javadoc block, like this:
    //   * The end of a javadoc block. */
    final Pattern javadocEndPattern = Pattern.compile(".*\\*/");

    final Puffin.Program program =
        Puffin.builder()
            .add(line -> line.fnr() == 1, line -> {
              state.fileCount++;
              state.starLine = 0;
              state.atLine = 0;
              state.javadocStartLine = 0;
              state.javadocEndLine = 0;
            })

            // Javadoc does not require '</p>'
            .add(line -> line.line.endsWith("</p>"),
                line -> state.message("no </p>", line))

            // A Javadoc paragraph '<p>' must not be on its own line.
            .add(line -> pEndPattern.matcher(line.line).matches(),
                line -> state.message("<p> must not be on its own line", line))

            // A Javadoc paragraph '<p>' must be preceded by a blank Javadoc
            // line.
            .add(line -> starPattern.matcher(line.line).matches(),
                line -> state.starLine = line.fnr())
            .add(line -> pPattern.matcher(line.line).matches()
                    && line.fnr() - 1 != state.starLine,
                line -> state.message("<p> must be preceded by blank line",
                    line))

            // The first "@param" of a javadoc block must be preceded by a blank
            // line.
            .add(line -> javadocStartPattern.matcher(line.line).matches(),
                line -> state.javadocStartLine = line.fnr())
            .add(line -> javadocEndPattern.matcher(line.line).matches(),
                line -> state.javadocEndLine = line.fnr())
            .add(line -> atPattern.matcher(line.line).matches(),
                line -> {
                  if (state.javadocEndLine < state.javadocStartLine
                      && state.atLine < state.javadocStartLine
                      && line.fnr() - 1 != state.starLine) {
                    state.message("First @tag must be preceded by blank line",
                        line);
                  }
                  state.atLine = line.fnr();
                })
            .build();

    try (PrintWriter pw = Util.printWriter(System.out)) {
      for (File file : new FileFixture().getJavaFiles()) {
        program.execute(Sources.of(file), pw);
      }
    }

    System.out.println("Lint: " + state.fileCount + " files,"
        + state.messages.size() + " warnings");
    for (Message message : state.messages) {
      System.out.println(message.source + ":"
          + message.line + ":" + message.message);
    }
  }

  /** A compiled regex and an operator name. An item to be found in the
   * documentation. */
  private static class PatternOp {
    final Pattern pattern;
    final String opName;

    private PatternOp(Pattern pattern, String opName) {
      this.pattern = pattern;
      this.opName = opName;
    }
  }

  /** Defines paths needed by a couple of tests. */
  private static class FileFixture {
    final File base;
    final File inFile;
    final File outFile;

    private boolean isProjectDir(File dir) {
      return new File(dir, "pom.xml").isFile()
          || new File(dir, "build.gradle.kts").isFile()
          || new File(dir, "gradle.properties").isFile();
    }

    FileFixture() {
      // Algorithm:
      // 1) Find location of DocumentationTest.class
      // 2) Climb via getParentFile() until we detect pom.xml
      // 3) It means we've got core/pom.xml, and we need to get core/../site/
      Class<DocumentationTest> klass = DocumentationTest.class;
      File docTestClass =
          Sources.of(klass.getResource(klass.getSimpleName() + ".class")).file();

      File core = docTestClass.getAbsoluteFile();
      for (int i = 0; i < 42; i++) {
        if (isProjectDir(core)) {
          // Ok, core == core/
          break;
        }
        core = core.getParentFile();
      }
      if (!isProjectDir(core)) {
        fail("Unable to find neither core/pom.xml nor core/build.gradle.kts. Started with "
            + docTestClass.getAbsolutePath()
            + ", the current path is " + core.getAbsolutePath());
      }
      base = core.getParentFile();
      inFile = new File(base, "site/_docs/reference.md");
      // TODO: replace with core/build/ when Maven is migrated to Gradle
      // It does work in Gradle, however, we don't want to create "target" folder in Gradle
      outFile = new File(base, "core/build/reports/documentationTest/reference.md");
    }

    /** Returns a list of Java files in git under a given directory.
     *
     * <p>Assumes running Linux or macOS, and that git is available. */
    List<File> getJavaFiles() {
      return ConcurrentTestCommandScript.getJavaFiles(base);
    }
  }
}
