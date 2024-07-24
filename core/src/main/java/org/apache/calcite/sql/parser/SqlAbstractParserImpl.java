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
package org.apache.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Abstract base for parsers generated from CommonParser.jj.
 */
public abstract class SqlAbstractParserImpl {
  //~ Static fields/initializers ---------------------------------------------

  protected static final Logger LOGGER = CalciteTrace.getParserTracer();

  // Can't use quoted literal because of a bug in how JavaCC translates
  // backslash-backslash.
  protected static final char BACKSLASH = 0x5c;
  protected static final char DOUBLE_QUOTE = 0x22;
  protected static final String DQ = DOUBLE_QUOTE + "";
  protected static final String DQDQ = DQ + DQ;
  protected static final SqlLiteral LITERAL_ZERO =
      SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
  protected static final SqlLiteral LITERAL_ONE =
      SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
  protected static final SqlLiteral LITERAL_MINUS_ONE =
      SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
  protected static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100L);
  private static final String DUMMY_STATEMENT = "1";
  private static final Pattern LEXICAL_ERROR_PATTERN =
      Pattern.compile("(?s)Lexical error at line ([0-9]+), column ([0-9]+).*");

  private static final ImmutableSet<String> SQL_92_RESERVED_WORD_SET =
      ImmutableSet.of(
          "ABSOLUTE",
          "ACTION",
          "ADD",
          "ALL",
          "ALLOCATE",
          "ALTER",
          "AND",
          "ANY",
          "ARE",
          "AS",
          "ASC",
          "ASSERTION",
          "AT",
          "AUTHORIZATION",
          "AVG",
          "BEGIN",
          "BETWEEN",
          "BIT",
          "BIT_LENGTH",
          "BOTH",
          "BY",
          "CALL",
          "CASCADE",
          "CASCADED",
          "CASE",
          "CAST",
          "CATALOG",
          "CHAR",
          "CHARACTER",
          "CHARACTER_LENGTH",
          "CHAR_LENGTH",
          "CHECK",
          "CLOSE",
          "COALESCE",
          "COLLATE",
          "COLLATION",
          "COLUMN",
          "COMMIT",
          "CONDITION",
          "CONNECT",
          "CONNECTION",
          "CONSTRAINT",
          "CONSTRAINTS",
          "CONTAINS",
          "CONTINUE",
          "CONVERT",
          "CORRESPONDING",
          "COUNT",
          "CREATE",
          "CROSS",
          "CURRENT",
          "CURRENT_DATE",
          "CURRENT_PATH",
          "CURRENT_TIME",
          "CURRENT_TIMESTAMP",
          "CURRENT_USER",
          "CURSOR",
          "DATE",
          "DAY",
          "DEALLOCATE",
          "DEC",
          "DECIMAL",
          "DECLARE",
          "DEFAULT",
          "DEFERRABLE",
          "DEFERRED",
          "DELETE",
          "DESC",
          "DESCRIBE",
          "DESCRIPTOR",
          "DETERMINISTIC",
          "DIAGNOSTICS",
          "DISCONNECT",
          "DISTINCT",
          "DOMAIN",
          "DOUBLE",
          "DROP",
          "ELSE",
          "END",
          "ESCAPE",
          "EXCEPT",
          "EXCEPTION",
          "EXEC",
          "EXECUTE",
          "EXISTS",
          "EXTERNAL",
          "EXTRACT",
          "FALSE",
          "FETCH",
          "FIRST",
          "FLOAT",
          "FOR",
          "FOREIGN",
          "FOUND",
          "FROM",
          "FULL",
          "FUNCTION",
          "GET",
          "GLOBAL",
          "GO",
          "GOTO",
          "GRANT",
          "GROUP",
          "HAVING",
          "HOUR",
          "IDENTITY",
          "IMMEDIATE",
          "IN",
          "INADD",
          "INDICATOR",
          "INITIALLY",
          "INNER",
          "INOUT",
          "INPUT",
          "INSENSITIVE",
          "INSERT",
          "INT",
          "INTEGER",
          "INTERSECT",
          "INTERVAL",
          "INTO",
          "IS",
          "ISOLATION",
          "JOIN",
          "KEY",
          "LANGUAGE",
          "LAST",
          "LEADING",
          "LEFT",
          "LEVEL",
          "LIKE",
          "LOCAL",
          "LOWER",
          "MATCH",
          "MAX",
          "MIN",
          "MINUTE",
          "MODULE",
          "MONTH",
          "NAMES",
          "NATIONAL",
          "NATURAL",
          "NCHAR",
          "NEXT",
          "NO",
          "NOT",
          "NULL",
          "NULLIF",
          "NUMERIC",
          "OCTET_LENGTH",
          "OF",
          "ON",
          "ONLY",
          "OPEN",
          "OPTION",
          "OR",
          "ORDER",
          "OUT",
          "OUTADD",
          "OUTER",
          "OUTPUT",
          "OVERLAPS",
          "PAD",
          "PARAMETER",
          "PARTIAL",
          "PATH",
          "POSITION",
          "PRECISION",
          "PREPARE",
          "PRESERVE",
          "PRIMARY",
          "PRIOR",
          "PRIVILEGES",
          "PROCEDURE",
          "PUBLIC",
          "READ",
          "REAL",
          "REFERENCES",
          "RELATIVE",
          "RESTRICT",
          "RETURN",
          "RETURNS",
          "REVOKE",
          "RIGHT",
          "ROLLBACK",
          "ROUTINE",
          "ROWS",
          "SCHEMA",
          "SCROLL",
          "SECOND",
          "SECTION",
          "SELECT",
          "SESSION",
          "SESSION_USER",
          "SET",
          "SIZE",
          "SMALLINT",
          "SOME",
          "SPACE",
          "SPECIFIC",
          "SQL",
          "SQLCODE",
          "SQLERROR",
          "SQLEXCEPTION",
          "SQLSTATE",
          "SQLWARNING",
          "SUBSTRING",
          "SUM",
          "SYSTEM_USER",
          "TABLE",
          "TEMPORARY",
          "THEN",
          "TIME",
          "TIMESTAMP",
          "TIMEZONE_HOUR",
          "TIMEZONE_MINUTE",
          "TO",
          "TRAILING",
          "TRANSACTION",
          "TRANSLATE",
          "TRANSLATION",
          "TRIM",
          "TRUE",
          "UNION",
          "UNIQUE",
          "UNKNOWN",
          "UPDATE",
          "UPPER",
          "USAGE",
          "USER",
          "USING",
          "VALUE",
          "VALUES",
          "VARCHAR",
          "VARYING",
          "VIEW",
          "WHEN",
          "WHENEVER",
          "WHERE",
          "WITH",
          "WORK",
          "WRITE",
          "YEAR",
          "ZONE");

  //~ Enums ------------------------------------------------------------------

  /**
   * Type-safe enum for context of acceptable expressions.
   */
  protected enum ExprContext {
    /**
     * Accept any kind of expression in this context.
     */
    ACCEPT_ALL,

    /**
     * Accept any kind of expression in this context, with the exception of
     * CURSOR constructors.
     */
    ACCEPT_NONCURSOR,

    /**
     * Accept only query expressions in this context.
     *
     * <p>Valid: "SELECT x FROM a",
     * "SELECT x FROM a UNION SELECT y FROM b",
     * "TABLE a",
     * "VALUES (1, 2), (3, 4)",
     * "(SELECT x FROM a UNION SELECT y FROM b) INTERSECT SELECT z FROM c",
     * "(SELECT x FROM a UNION SELECT y FROM b) ORDER BY 1 LIMIT 10".
     * Invalid: "e CROSS JOIN d".
     * Debatable: "(SELECT x FROM a)".
     */
    ACCEPT_QUERY,

    /**
     * Accept only query expressions or joins in this context.
     *
     * <p>Valid: "(SELECT x FROM a)",
     * "e CROSS JOIN d",
     * "((SELECT x FROM a) CROSS JOIN d)",
     * "((e CROSS JOIN d) LEFT JOIN c)".
     * Invalid: "e, d",
     * "SELECT x FROM a",
     * "(e)".
     */
    ACCEPT_QUERY_OR_JOIN,

    /**
     * Accept only non-query expressions in this context.
     */
    ACCEPT_NON_QUERY,

    /**
     * Accept only parenthesized queries or non-query expressions in this
     * context.
     */
    ACCEPT_SUB_QUERY,

    /**
     * Accept only CURSOR constructors, parenthesized queries, or non-query
     * expressions in this context.
     */
    ACCEPT_CURSOR;

    @Deprecated // to be removed before 2.0
    public static final ExprContext ACCEPT_SUBQUERY = ACCEPT_SUB_QUERY;

    @Deprecated // to be removed before 2.0
    public static final ExprContext ACCEPT_NONQUERY = ACCEPT_NON_QUERY;

    public void throwIfNotCompatible(SqlNode e) {
      switch (this) {
      case ACCEPT_NON_QUERY:
      case ACCEPT_SUB_QUERY:
      case ACCEPT_CURSOR:
        if (e.isA(SqlKind.QUERY)) {
          throw SqlUtil.newContextException(e.getParserPosition(),
              RESOURCE.illegalQueryExpression());
        }
        break;
      case ACCEPT_QUERY:
        if (!e.isA(SqlKind.QUERY)) {
          throw SqlUtil.newContextException(e.getParserPosition(),
              RESOURCE.illegalNonQueryExpression());
        }
        break;
      case ACCEPT_QUERY_OR_JOIN:
        if (!e.isA(SqlKind.QUERY) && e.getKind() != SqlKind.JOIN) {
          throw SqlUtil.newContextException(e.getParserPosition(),
              RESOURCE.expectedQueryOrJoinExpression());
        }
        break;
      default:
        break;
      }
    }
  }

  //~ Instance fields --------------------------------------------------------

  protected int nDynamicParams;

  protected @Nullable String originalSql;

  protected final List<CalciteContextException> warnings = new ArrayList<>();

  protected @Nullable Casing unquotedCasing;
  protected @Nullable Casing quotedCasing;
  protected int identifierMaxLength;
  protected @Nullable SqlConformance conformance;

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns immutable set of all reserved words defined by SQL-92.
   *
   * @see Glossary#SQL92 SQL-92 Section 5.2
   */
  public static Set<String> getSql92ReservedWords() {
    return SQL_92_RESERVED_WORD_SET;
  }

  /**
   * Creates a call.
   *
   * @param funName           Name of function
   * @param pos               Position in source code
   * @param funcType          Type of function
   * @param functionQualifier Qualifier
   * @param operands          Operands to call
   * @return Call
   */
  @SuppressWarnings("argument.type.incompatible")
  protected SqlCall createCall(
      SqlIdentifier funName,
      SqlParserPos pos,
      SqlFunctionCategory funcType,
      SqlLiteral functionQualifier,
      Iterable<? extends SqlNode> operands) {
    return createCall(funName, pos, funcType, functionQualifier,
        Iterables.toArray(operands, SqlNode.class));
  }

  /**
   * Creates a call.
   *
   * @param funName           Name of function
   * @param pos               Position in source code
   * @param funcType          Type of function
   * @param functionQualifier Qualifier
   * @param operands          Operands to call
   * @return Call
   */
  protected SqlCall createCall(
      SqlIdentifier funName,
      SqlParserPos pos,
      SqlFunctionCategory funcType,
      SqlLiteral functionQualifier,
      SqlNode[] operands) {
    // Create a placeholder function.  Later, during
    // validation, it will be resolved into a real function reference.
    SqlOperator fun = new SqlUnresolvedFunction(funName, null, null, null, null,
        funcType);

    return fun.createCall(functionQualifier, pos, operands);
  }

  /**
   * Returns metadata about this parser: keywords, etc.
   */
  public abstract Metadata getMetadata();

  /**
   * Returns the tokens following a symbol.
   */
  protected abstract Pair<int[][], String[]> call(String name);

  /**
   * Removes or transforms misleading information from a parse exception or
   * error, and converts to {@link SqlParseException}.
   *
   * <p>Default implementation can only handle {@link CalciteContextException}
   * and {@link SqlParseException}. Generated subclasses (the actual parsers)
   * should override to handle generated exception classes
   * ({@code ParseException} and {@code TokenMgrError}), then call
   * {@code super.normalizeException} for other exception types.
   *
   * @param ex Dirty exception
   * @return Clean exception
   */
  public SqlParseException normalizeException(Throwable ex) {
    if (ex instanceof SqlParseException) {
      return (SqlParseException) ex;
    }

    if (ex instanceof CalciteContextException) {
      // CalciteContextException is the standard wrapper for exceptions
      // produced by the validator, but in the parser, the standard is
      // SqlParseException; so, strip it away. In case you were wondering,
      // the CalciteContextException appears because the parser
      // occasionally calls into validator-style code such as
      // SqlSpecialOperator.reduceExpr.
      CalciteContextException ece =
          (CalciteContextException) ex;
      if (originalSql != null) {
        ece.setOriginalStatement(originalSql);
      }
      SqlParserPos pos =
          new SqlParserPos(ece.getPosLine(), ece.getPosColumn(),
              ece.getEndPosLine(), ece.getEndPosColumn());
      Throwable cause = requireNonNull(ece.getCause(), "cause");
      requireNonNull(cause.getMessage());
      return new SqlParseException(cause.getMessage(), pos, null, null, cause);
    }

    // Unknown exception (might be IllegalArgumentException,
    // CalciteException). Wrap it in SqlParseException.
    requireNonNull(ex.getMessage());
    return new SqlParseException(ex.getMessage(), null, null, null, ex);
  }

  protected SqlParseException normalizeParseException(WrappedParseException w) {
    cleanupParseException(w);

    final SqlParserPos pos = w.pos();
    final int[][] expectedTokenSequences = w.expectedTokenSequences();
    final String[] tokenImages = w.tokenImages();
    final String image = w.image();
    Throwable ex = w.ex();
    if (image != null
        && expectedTokenSequences != null
        && getMetadata().isKeyword(image)
        && SqlParserUtil.allowsIdentifier(tokenImages, expectedTokenSequences)) {
      // If the next token is a keyword, reformat the error message as
      // follows:
      //   Incorrect syntax near the keyword '{keyword}' at
      //   line {line_number}, column {column_number}.
      final String message = requireNonNull(w.ex().getMessage());
      final String expecting =
          message.substring(message.indexOf("Was expecting"));
      final String message2 =
          String.format("Incorrect syntax near the keyword '%s' "
                  + "at line %d, column %d.\n%s",
              image, pos.getLineNum(), pos.getColumnNum(), expecting);

      // Replace the ParseException with explicit error message.
      ex = w.copy(message2);
    }

    requireNonNull(ex.getMessage());
    return new SqlParseException(ex.getMessage(), pos, expectedTokenSequences,
        tokenImages, ex);
  }

  protected void cleanupParseException(WrappedParseException w) {
    final int @Nullable [][] expectedTokenSequences =
        w.expectedTokenSequences();
    if (expectedTokenSequences == null) {
      return;
    }
    final String[] tokenImages = w.tokenImages();

    // Find all sequences in the error which contain identifier. For
    // example,
    //       {<IDENTIFIER>}
    //       {A}
    //       {B, C}
    //       {D, <IDENTIFIER>}
    //       {D, A}
    //       {D, B}
    //
    // would yield
    //       {}
    //       {D}
    final List<int[]> prefixList = new ArrayList<>();
    for (int[] seq : expectedTokenSequences) {
      if (tokenImages[seq[seq.length - 1]].equals("<IDENTIFIER>")) {
        int[] prefix = Arrays.copyOf(seq, seq.length - 1);
        prefixList.add(prefix);
      }
    }

    if (prefixList.isEmpty()) {
      return;
    }

    int[][] prefixes = prefixList.toArray(new int[0][]);

    // Since <IDENTIFIER> was one of the possible productions,
    // we know that the parser will also have included all
    // the non-reserved keywords (which are treated as
    // identifiers in non-keyword contexts).  So, now we need
    // to clean those out, since they're totally irrelevant.

    final List<int[]> list = new ArrayList<>();
    final Metadata metadata = getMetadata();
    for (int[] seq : expectedTokenSequences) {
      String tokenImage = tokenImages[seq[seq.length - 1]];
      String token = SqlParserUtil.getTokenVal(tokenImage);
      if (token == null || !metadata.isNonReservedKeyword(token)) {
        list.add(seq);
        continue;
      }
      boolean match = matchesPrefix(seq, prefixes);
      if (!match) {
        list.add(seq);
      }
    }

    w.setExpectedTokenSequences(list.toArray(new int[0][]));
  }

  protected SqlParseException normalizeTokenMgrError(Error ex) {
    final String message = ex.getMessage();
    requireNonNull(message);
    final SqlParserPos pos = getSqlParserPos(message);
    return new SqlParseException(message, pos, null, null, ex);
  }

  /** Returns an error location in an error message. The message must look
   * something like
   *
   * <blockquote><pre>
   * Lexical error at line 3, column 24.  Encountered "#" after "a".
   * </pre></blockquote> */
  private static SqlParserPos getSqlParserPos(String message) {
    final Matcher matcher = LEXICAL_ERROR_PATTERN.matcher(message);
    if (!matcher.matches()) {
      return SqlParserPos.ZERO;
    }
    final String group1 = matcher.group(1);
    final String group2 = matcher.group(2);
    if (group1 == null || group2 == null) {
      return SqlParserPos.ZERO;
    }
    int line = Integer.parseInt(group1);
    int column = Integer.parseInt(group2);
    return new SqlParserPos(line, column, line, column);
  }

  protected abstract SqlParserPos getPos() throws Exception;

  /**
   * Reinitializes parser with new input.
   *
   * @param reader provides new input
   */
  // CHECKSTYLE: IGNORE 1
  public abstract void ReInit(Reader reader);

  /**
   * Parses a SQL expression ending with EOF and constructs a
   * parse tree.
   *
   * @return constructed parse tree.
   */
  public abstract SqlNode parseSqlExpressionEof() throws Exception;

  /**
   * Parses a SQL statement ending with EOF and constructs a
   * parse tree.
   *
   * @return constructed parse tree.
   */
  public abstract SqlNode parseSqlStmtEof() throws Exception;

  /**
   * Parses a list of SQL statements separated by semicolon and constructs a
   * parse tree. The semicolon is required between statements, but is
   * optional at the end.
   *
   * @return constructed list of SQL statements.
   */
  public abstract SqlNodeList parseSqlStmtList() throws Exception;

  /**
   * Sets the tab stop size.
   *
   * @param tabSize Tab stop size
   */
  public abstract void setTabSize(int tabSize);

  /**
   * Sets the casing policy for quoted identifiers.
   *
   * @param quotedCasing Casing to set.
   */
  public void setQuotedCasing(Casing quotedCasing) {
    this.quotedCasing = quotedCasing;
  }

  /**
   * Sets the casing policy for unquoted identifiers.
   *
   * @param unquotedCasing Casing to set.
   */
  public void setUnquotedCasing(Casing unquotedCasing) {
    this.unquotedCasing = unquotedCasing;
  }

  /**
   * Sets the maximum length for sql identifier.
   */
  public void setIdentifierMaxLength(int identifierMaxLength) {
    this.identifierMaxLength = identifierMaxLength;
  }


  /**
   * Sets the map from identifier to time unit.
   */
  @Deprecated // to be removed before 2.0
  public void setTimeUnitCodes(Map<String, TimeUnit> timeUnitCodes) {
  }

  /**
   * Sets the SQL language conformance level.
   */
  public void setConformance(SqlConformance conformance) {
    this.conformance = conformance;
  }

  /**
   * Parses string to array literal.
   */
  public abstract SqlNode parseArray() throws SqlParseException;

  /**
   * Sets the SQL text that is being parsed.
   */
  public void setOriginalSql(String originalSql) {
    this.originalSql = originalSql;
  }

  /**
   * Returns the SQL text.
   */
  public @Nullable String getOriginalSql() {
    return originalSql;
  }

  /**
   * Change parser state.
   *
   * @param state New state
   */
  public abstract void switchTo(LexicalState state);

  //~ Inner Interfaces -------------------------------------------------------

  /** Valid starting states of the parser.
   *
   * <p>(There are other states that the parser enters during parsing, such as
   * being inside a multi-line comment.)
   *
   * <p>The starting states generally control the syntax of quoted
   * identifiers. */
  public enum LexicalState {
    /** Starting state where quoted identifiers use brackets, like Microsoft SQL
     * Server. */
    DEFAULT,

    /** Starting state where quoted identifiers use double-quotes, like
     * Oracle and PostgreSQL. */
    DQID,

    /** Starting state where quoted identifiers use back-ticks, like MySQL. */
    BTID,

    /** Starting state where quoted identifiers use back-ticks,
     * unquoted identifiers that are part of table names may contain hyphens,
     * and character literals may be enclosed in single- or double-quotes,
     * like BigQuery. */
    BQID;

    /** Returns the corresponding parser state with the given configuration
     * (in particular, quoting style). */
    public static LexicalState forConfig(SqlParser.Config config) {
      switch (config.quoting()) {
      case BRACKET:
        return DEFAULT;
      case DOUBLE_QUOTE:
        return DQID;
      case BACK_TICK_BACKSLASH:
        return BQID;
      case BACK_TICK:
        if (config.conformance().allowHyphenInUnquotedTableName()
            && config.charLiteralStyles().equals(
                EnumSet.of(CharLiteralStyle.BQ_SINGLE,
                    CharLiteralStyle.BQ_DOUBLE))) {
          return BQID;
        }
        if (!config.conformance().allowHyphenInUnquotedTableName()
            && config.charLiteralStyles().equals(
                EnumSet.of(CharLiteralStyle.STANDARD))) {
          return BTID;
        }
        // fall through
      default:
        throw new AssertionError(config);
      }
    }
  }

  /**
   * Metadata about the parser. For example:
   *
   * <ul>
   * <li>"KEY" is a keyword: it is meaningful in certain contexts, such as
   * "CREATE FOREIGN KEY", but can be used as an identifier, as in <code>
   * "CREATE TABLE t (key INTEGER)"</code>.
   * <li>"SELECT" is a reserved word. It can not be used as an identifier.
   * <li>"CURRENT_USER" is the name of a context variable. It cannot be used
   * as an identifier.
   * <li>"ABS" is the name of a reserved function. It cannot be used as an
   * identifier.
   * <li>"DOMAIN" is a reserved word as specified by the SQL:92 standard.
   * </ul>
   */
  public interface Metadata {
    /**
     * Returns true if token is a keyword but not a reserved word. For
     * example, "KEY".
     */
    boolean isNonReservedKeyword(String token);

    /**
     * Returns whether token is the name of a context variable such as
     * "CURRENT_USER".
     */
    boolean isContextVariableName(String token);

    /**
     * Returns whether token is a reserved function name such as
     * "CURRENT_USER".
     */
    boolean isReservedFunctionName(String token);

    /**
     * Returns whether token is a keyword. (That is, a non-reserved keyword,
     * a context variable, or a reserved function name.)
     */
    boolean isKeyword(String token);

    /**
     * Returns whether token is a reserved word.
     */
    boolean isReservedWord(String token);

    /**
     * Returns whether token is a reserved word as specified by the SQL:92
     * standard.
     */
    boolean isSql92ReservedWord(String token);

    /**
     * Returns comma-separated list of JDBC keywords.
     */
    String getJdbcKeywords();

    /**
     * Returns a list of all tokens in alphabetical order.
     */
    List<String> getTokens();
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Default implementation of the {@link Metadata} interface.
   */
  public static class MetadataImpl implements Metadata {
    /**
     * Immutable map of all tokens, in alphabetical order.
     */
    private final Map<String, ImmutableSet<KeywordType>> tokenMap;
    private final String sql92ReservedWords;

    /**
     * Creates a MetadataImpl.
     *
     * @param sqlParser Parser
     */
    public MetadataImpl(SqlAbstractParserImpl sqlParser) {
      final NavigableMap<String, EnumSet<KeywordType>> tokenSet = new TreeMap<>();
      initList(sqlParser, tokenSet, "ReservedFunctionName",
          w -> tokenSet.get(w).add(KeywordType.RESERVED_FUNCTION_NAME));
      initList(sqlParser, tokenSet, "ContextVariable",
          w -> tokenSet.get(w).add(KeywordType.CONTEXT_VARIABLE_NAME));
      initList(sqlParser, tokenSet, "NonReservedKeyWord",
          w -> tokenSet.get(w).add(KeywordType.NON_RESERVED_KEYWORD));
      tokenSet.forEach((key, value) -> {
        if (!value.contains(KeywordType.NON_RESERVED_KEYWORD)) {
          value.add(KeywordType.RESERVED_WORD);
        }
      });

      // Maps each combination of keyword types to an immutable set.
      final Map<EnumSet<KeywordType>, ImmutableSet<KeywordType>> canonicalMap =
          new HashMap<>();
      final ImmutableMap.Builder<String, ImmutableSet<KeywordType>> builder =
          ImmutableMap.builder();
      tokenSet.forEach((key, value) ->
          builder.put(key,
              canonicalMap.computeIfAbsent(value, ImmutableSet::copyOf)));
      tokenMap = builder.build();

      // Build a comma-separated list of JDBC reserved words.
      final Set<String> jdbcReservedSet = new TreeSet<>(tokenMap.keySet());
      jdbcReservedSet.removeAll(SQL_92_RESERVED_WORD_SET);
      jdbcReservedSet.removeIf(w -> {
        final ImmutableSet<KeywordType> keywordTypes = tokenMap.get(w);
        return keywordTypes == null
            || keywordTypes.contains(KeywordType.NON_RESERVED_KEYWORD);
      });
      sql92ReservedWords = commaList(jdbcReservedSet);
    }

    /**
     * Initializes lists of keywords.
     */
    private static void initList(SqlAbstractParserImpl parserImpl,
        NavigableMap<String, EnumSet<KeywordType>> tokenSet, String name,
        Consumer<String> keywords) {
      parserImpl.ReInit(new StringReader(DUMMY_STATEMENT));
      Pair<int[][], String[]> p = parserImpl.call(name);
      final int[][] expectedTokenSequences = requireNonNull(p.left);
      final String[] tokenImages = requireNonNull(p.right);

      // First time through, build the list of all tokens.
      requireNonNull(tokenImages);
      if (tokenSet.isEmpty()) {
        for (String token : tokenImages) {
          String tokenVal = SqlParserUtil.getTokenVal(token);
          if (tokenVal != null) {
            tokenSet.put(tokenVal, EnumSet.of(KeywordType.TOKEN));
          }
        }
      }

      // Add the tokens which would have been expected in this
      // syntactic context to the list we're building.
      requireNonNull(expectedTokenSequences);
      for (final int[] tokens : expectedTokenSequences) {
        assert tokens.length == 1;
        final int tokenId = tokens[0];
        String token = tokenImages[tokenId];
        String tokenVal = SqlParserUtil.getTokenVal(token);
        if (tokenVal != null) {
          keywords.accept(tokenVal);
        }
      }
    }

    @Override public List<String> getTokens() {
      // This operation is efficient. tokenMap is an ImmutableMap, so its
      // keySet can be viewed as an ImmutableList.
      return ImmutableList.copyOf(tokenMap.keySet());
    }

    @Override public boolean isSql92ReservedWord(String token) {
      return SQL_92_RESERVED_WORD_SET.contains(token);
    }

    @Override public String getJdbcKeywords() {
      return sql92ReservedWords;
    }

    @Override public boolean isKeyword(String token) {
      final ImmutableSet<KeywordType> keywordTypes = tokenMap.get(token);
      if (keywordTypes == null) {
        return false;
      }
      return keywordTypes.contains(KeywordType.NON_RESERVED_KEYWORD)
          || keywordTypes.contains(KeywordType.RESERVED_FUNCTION_NAME)
          || keywordTypes.contains(KeywordType.CONTEXT_VARIABLE_NAME)
          || keywordTypes.contains(KeywordType.RESERVED_WORD);
    }

    @Override public boolean isNonReservedKeyword(String token) {
      final ImmutableSet<KeywordType> keywordTypes = tokenMap.get(token);
      if (keywordTypes == null) {
        return false;
      }
      return keywordTypes.contains(KeywordType.NON_RESERVED_KEYWORD);
    }

    @Override public boolean isReservedFunctionName(String token) {
      final ImmutableSet<KeywordType> keywordTypes = tokenMap.get(token);
      if (keywordTypes == null) {
        return false;
      }
      return keywordTypes.contains(KeywordType.RESERVED_FUNCTION_NAME);
    }

    @Override public boolean isContextVariableName(String token) {
      final ImmutableSet<KeywordType> keywordTypes = tokenMap.get(token);
      if (keywordTypes == null) {
        return false;
      }
      return keywordTypes.contains(KeywordType.CONTEXT_VARIABLE_NAME);
    }

    @Override public boolean isReservedWord(String token) {
      final ImmutableSet<KeywordType> keywordTypes = tokenMap.get(token);
      if (keywordTypes == null) {
        return false;
      }
      return keywordTypes.contains(KeywordType.RESERVED_WORD);
    }
  }

  /** Describes the keyword categories that a token belongs to. */
  private enum KeywordType {
    RESERVED_FUNCTION_NAME,
    CONTEXT_VARIABLE_NAME,
    NON_RESERVED_KEYWORD,
    TOKEN,
    RESERVED_WORD
  }

  /** Converts a collection of strings to a comma-separated list.
   *
   * <p>{@code commaList(["a", "bc"]} yields "a,bc". */
  private static String commaList(Iterable<String> strings) {
    StringBuilder sb = new StringBuilder();
    for (String string : strings) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(string);
    }
    return sb.toString();
  }

  protected static boolean matchesPrefix(int[] seq, int[][] prefixes) {
    for (int[] prefix : prefixes) {
      if (allMatch(seq, prefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean allMatch(int[] seq, int[] prefix) {
    if (seq.length != prefix.length + 1) {
      return false;
    }
    for (int k = 0; k < prefix.length; k++) {
      if (prefix[k] != seq[k]) {
        return false;
      }
    }
    return true;
  }

  /** Abstract implementation of {@link SqlParserImplFactory}. */
  protected abstract static class AbstractFactory
      implements SqlParserImplFactory {
    private final Supplier<Metadata> metadataSupplier =
        Suppliers.memoize(() ->
            new MetadataImpl(create(new StringReader(""))));

    @Override public SqlAbstractParserImpl getParser(Reader reader) {
      final SqlAbstractParserImpl parser = create(reader);
      if (reader instanceof SourceStringReader) {
        parser.setOriginalSql(((SourceStringReader) reader).getSourceString());
      }
      return parser;
    }

    /** Creates a parser. */
    protected abstract SqlAbstractParserImpl create(
        @UnknownInitialization AbstractFactory this, Reader reader);

    /** Returns metadata, caching after the first call. */
    @Override public Metadata getMetadata() {
      return metadataSupplier.get();
    }
  }

  /** Wrapper around a parse exception. */
  protected interface WrappedParseException {
    int @Nullable [][] expectedTokenSequences();
    void setExpectedTokenSequences(int[][] expectedTokenSequences);
    String[] tokenImages();
    SqlParserPos pos();
    @Nullable String image();
    Throwable ex();

    /** Returns a same copy of the underlying exception, with the same type
     * but with the given message. */
    Throwable copy(String message);
  }
}
