## Description of change [CALCITE-4885]

The following is an overview of the refactorings made as part of
[[CALCITE-4885](https://issues.apache.org/jira/browse/CALCITE-4885)].
This document will evolve as we work on the PR, and when the PR is
merged the document will be deleted and its contents will become the
commit message.

# TODO

* obsolete SqlToRelTestBase?
* rename SqlPrettyWriterTestFixture to SqlPrettyWriterFixture
* static import Assertions.assertEquals, Objects.requireNonNull
* rename SqlFixture to SqlOperatorFixture, ditto SqlFixtureImpl, and fix javadoc
* rename TryThreadLocal.withValue to let

# Goal and strategy

The goal is to be able to write tests from both inside and outside of
Calcite, and without deriving from a particular test class.

We achieve that goal by creating *fixture* objects for each kind of
test. Test configuration (e.g. what SQL query to test) belongs in that
fixture, and is set using wither methods.

Inside the fixture is a *factory* that creates the objects necessary
to run the test (parser, validator, and so forth). The factory
necssarily contains the config for the parser, validator, etc. Tests
configure the factory by calling methods in their fixture.

Also inside a fixture is a *tester* that orchestrates the lifecycle
(parse, validate, SQL-to-rel, check results). The tester is stateless
(necessary state is provided by parameters to its methods, which often
include a `SqlTestFactory`). There is one main implementation of the
tester, but it can be substituted with an radically different
implementations (say one that no-ops, or dumps all SQL expressions to
a file).

Tests that compare actual results with expected results may have a
`DiffRepository` attached to their fixture.

# Remove deprecated methods

The code was deprecated in [CALCITE-4591], [CALCITE-4593],
[CALCITE-4446] to be removed before release 1.28.

# Add `class Fixtures`

Class `Fixtures` is a single place to obtain fixtures to tests of the
parser, validator, operators, planner rules, and more. These fixtures
can be used within Calcite but also by any project that uses Calcite
(the project just needs to use the `testkit` module).

`class FixtureTest` tests `Fixtures` and contains examples.

Add `fixture()` methods, to create fixtures without SQL, to many test
classes including `RelToSqlConverterTest`, `InterpreterTest`,
`SqlParserTest`, `SqlValidatorTest`, `DruidAdapter2IT`,
`DruidAdapterIT`. (Previously many tests would write `sql("?")` to
create tests with dummy SQL.

In `class CalciteAssert`, move method `withRel` from `AssertQuery` to
`AssertThat`, so that tests can provide a `RelNode` without first
providing dummy SQL. Also make the list of hooks (used heavily by
`AssertQuery`) immutable.

# Refactor `interface SqlTester`

In `SqlTester`, remove methods `checkFieldOrigin`, `checkResultType`,
`checkCollation`, `checkCharset`, `checkIntervalConv`,
`checkMonotonic`; all can now be implemented in terms of a new method
`validateAndThen`, and similar methods `validateAndApply` and
`forEachQueryValidateAndThen`.

# `class SqlToRelTestBase`

Obsolete `interface SqlToRelTestBase.Tester`; `SqlToRelFixture` now
uses `SqlTester`.

Move inner `class MockViewExpander` from `SqlToRelTestBase` to
`SqlTestFactory`.

Method `SqlToRelTestBase.assertValid` becomes `Matchers.relIsValid`.

# Rename `interface SqlValidatorTestCase.Sql` to `SqlValidatorFixture`

Refactor/rename a few methods, and change 'query' field to 'expression'.

# Remove state from `class SqlParserTest`

State is now in the fixture (`class SqlParserFixture`) or
`SqlTestFactory`.

Create a fixture (`class SqlParserListFixture`) for list-based parser tests.

Remove fixture's `transform` field; config is now transformed
immediately, not deferred.

Remove field `LINUXIFY` (thread-local state).

# Move classes to `testkit` module

Move classes `DiffRepository`, `MockRelOptPlanner`,
`SqlToRelTestBase`, `RelOptTestBase`, `SqlFixtureImpl`,
`SqlRuntimeTester`,
`SqlOperatorTest` (renaming to `CoreSqlOperatorTest`),
`SqlOperatorBaseTest` (renaming to `SqlOperatorTest`).

# Rename `class SqlToRelConverterTest.Sql` to `class SqlToRelFixture`

Fields `SqlToRelFixture.expression` and `SqlValidatorFixture.expression`
were each previously called `query` and had the opposite sense;
both are now consistent with `SqlParserFixture.expression`.

Rename method `sql` to `withSql`, `config` to `withConfig`, etc.

# Rename `class RelOptTestBase.Sql` to `RelOptFixture`

Rename method `withDecorrelation` to `withDecorrelate`,
`withLateDecorrelation` to `withLateDecorrelate`
`with(RelOptPlanner)` to `withPlanner`,
`with(HepProgram)` to `withProgram`.

# Rename `class RelMetadataTest.Sql` to `RelMetadataFixture`

# Rename `class AbstractMaterializedViewTest` to `MaterializedViewTester`

`MaterializedViewTester` is now a utility class for materialized view
tests but is no longer required to be a base class.

Move `interface AbstractMaterializedViewTest.Sql` to top-level
`class MaterializedViewFixture`.

In `class MaterializedViewSubstitutionVisitorTest`, create a fixture
for satisfiability tests.

# Move `interface RelSupplier` to top-level

`RelSupplier` was previously an inner class of `class RelOptTestBase`.

It is used in metadata tests (`RelMetadataFixture`) and planner rule
tests (`RelOptFixture`) but could be used in other tests too.

Add `class RelSuppliers` with utilities and implementations for
`RelSupplier`.

# `class SqlAdvisorTest`

Move `assertXxx` methods into a fixture, `class
SqlAdvisorTest.Fixture`.

# `class SqlValidatorTestCase`

Remove inner `class LexConfiguration`; it relied on a mutable tester
in each `SqlValidatorTest` instance, which is no longer there.

# `class StringAndPos`

Implement methods `toString`, `equals`, `hashCode`.

# `class SqlPrettyWriterTest`

Move inner `class Sql` to top-level `class
SqlPrettyWriterTestFixture`.

# Move `interface CalciteAssert.ConnectionFactory` to top-level `interface ConnectionFactory`

Add `class ConnectionFactories`, utilities for `ConnectionFactory` and
`CalciteAssert.ConnectionPostProcessor`.

# New `interface JdbcType`

Enumerates all possible 'xxx' in JDBC `ResultSet.getXxx` and
`PreparedStatement.setXxx` methods.

# New `class ResultCheckers`

Utilities for `interface SqlTester.ResultChecker`.

# `class SqlOperatorTest`

Move `interface SqlOperatorTest.Fixture` to `SqlOperatorFixture`.

In `class SqlOperatorFixture`, remove `double delta` arguments from
methods that test floating-point values; instead use a `ResultChecker`
created using a `ResultCheckers` method such as `isWithin` or
`isExactly`.

Remove method `withLibrary2`.

# Other

Move `class SqlToRelConverterTest.RelValidityChecker` to top-level.

In class `SqlLimitsTest`, move method `checkTypes` to
`class SqlTests`.

In a few cases we would built complex transforms (instances of
`UnaryOperator`) that were applied just before the test ran. Now we
try to apply transforms early, storing a transformed config rather
than a chained transform. It's easier to understand and debug.

Remove method `RelDataTypeSystemImpl.allowExtendedTrim`, which was
added in [CALCITE-2571] but was never used.

Remove some uses of `assert` in `TypeCoercionTest`. (Never use
`assert` in tests!)

# Changes in non-test code

In `class SqlValidator.Config`, rename method `sqlConformance()` to
`conformance()`, to be consistent with conformance properties
elsewhere.

In `class TryThreadLocal`, add methods `withValue(T, Supplier)` and
`withValue(T, Runnable)`.
