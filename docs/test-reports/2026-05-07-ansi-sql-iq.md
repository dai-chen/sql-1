# ANSI SQL Quidem IT Report

Executed 26 .iq files: 0 passed, 26 failed.

**Query-level: 264/1867 queries passed (14.1%)**

Each .iq file is a Quidem golden-file test adapted from Apache Calcite. A file passes when the expected-output blocks in the file match exactly what Quidem produces after replaying every query through the OpenSearch unified path (force_routing=true → Calcite-native planner → analytics-engine execution).

## Per-file query results

| File | Total | Pass | Fail | Rate |
|---|---:|---:|---:|---:|
| agg.iq | 231 | 9 | 222 | 4% |
| cast-with-format.iq | 361 | 222 | 139 | 61% |
| cast.iq | 122 | 2 | 120 | 2% |
| conditions.iq | 32 | 1 | 31 | 3% |
| dual-table-query.iq | 4 | 1 | 3 | 25% |
| functions.iq | 95 | 2 | 93 | 2% |
| join.iq | 54 | 1 | 53 | 2% |
| lateral.iq | 11 | 0 | 11 | 0% |
| misc.iq | 169 | 2 | 167 | 1% |
| new-decorr.iq | 19 | 0 | 19 | 0% |
| operator.iq | 60 | 0 | 60 | 0% |
| outer.iq | 21 | 0 | 21 | 0% |
| pivot.iq | 31 | 2 | 29 | 6% |
| qualify.iq | 6 | 0 | 6 | 0% |
| recursive_queries.iq | 16 | 0 | 16 | 0% |
| scalar.iq | 23 | 0 | 23 | 0% |
| sequence.iq | 3 | 1 | 2 | 33% |
| set-op.iq | 18 | 0 | 18 | 0% |
| some.iq | 51 | 0 | 51 | 0% |
| sort.iq | 30 | 1 | 29 | 3% |
| sub-query.iq | 382 | 5 | 377 | 1% |
| tablesample.iq | 2 | 0 | 2 | 0% |
| unnest.iq | 18 | 1 | 17 | 6% |
| unsigned.iq | 25 | 0 | 25 | 0% |
| winagg.iq | 49 | 9 | 40 | 18% |
| within-distinct.iq | 34 | 5 | 29 | 15% |
| **TOTAL** | **1867** | **264** | **1603** | **14.1%** |

## Per-file status

✗ agg.iq
✗ cast-with-format.iq
✗ cast.iq
✗ conditions.iq
✗ dual-table-query.iq
✗ functions.iq
✗ join.iq
✗ lateral.iq
✗ misc.iq
✗ new-decorr.iq
✗ operator.iq
✗ outer.iq
✗ pivot.iq
✗ qualify.iq
✗ recursive_queries.iq
✗ scalar.iq
✗ sequence.iq
✗ set-op.iq
✗ some.iq
✗ sort.iq
✗ sub-query.iq
✗ tablesample.iq
✗ unnest.iq
✗ unsigned.iq
✗ winagg.iq
✗ within-distinct.iq

## Failure causes (one per file, categorized)

| Cause | Files |
|---|---:|
| JDBC: Error executing query (see HTML report for server-side cause) | 16 |
| output differs from expected (query succeeded) | 2 |
| other: Error while executing command ErrorCommand [sql: select 2... | 1 |
| other: Error while executing command OkCommand [sql: WITH t(x) A... | 1 |
| other: Error while executing command OkCommand [sql: SELECT 1 + ... | 1 |
| other: Error while executing command OkCommand [sql: select bit_... | 1 |
| other:  | 1 |
| other: Error while executing command OkCommand [sql: select next... | 1 |
| other: Error while executing command VerifyCommand [sql: select ... | 1 |
| other: java.sql.SQLFeatureNotSupportedException: Updates are not... | 1 |

## Failure details (first diff per file)

agg.iq:
      line 24:
        expected: | C |
        actual:   | c |
      line 35:
        expected: | C |
        actual:   | c |
      line 46:
        expected: | C |
        actual:   | c |
      line 56:
        expected: +---+
        actual:   java.sql.SQLException: Error executing query
      line 57:
        expected: | C |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 58:
        expected: +---+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 59:
        expected: | 8 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 60:
        expected: +---+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 61:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 62:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 63:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 64:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 65:
        expected: # DISTINCT and GROUP BY
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 66:
        expected: select distinct deptno, count(*) as c from emp group by deptno;
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 67:
        expected: +--------+---+
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 68:
        expected: | DEPTNO | C |
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 69:
        expected: +--------+---+
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 70:
        expected: |     10 | 2 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 71:
        expected: |     20 | 1 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 72:
        expected: |     30 | 2 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 73:
        expected: |     50 | 2 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 74:
        expected: |     60 | 1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 75:
        expected: |        | 1 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 76:
        expected: +--------+---+
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 77:
        expected: (6 rows)
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 78:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 79:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 80:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 81:
        expected: select distinct deptno from emp group by deptno;
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 82:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 83:
        expected: | DEPTNO |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 84:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 85:
        expected: |     10 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 86:
        expected: |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 87:
        expected: |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 88:
        expected: |     50 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 89:
        expected: |     60 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 90:
        expected: |        |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 91:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 92:
        expected: (6 rows)
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      ... (more differences truncated)

cast-with-format.iq:
      line 31:
        expected: EXPR$0
        actual:   java.sql.SQLException: Error executing query
      line 32:
        expected: 2017-05-01 01:23:45
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 33:
        expected: !ok
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 34:
        expected: 
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 35:
        expected: # Input that contains shuffled date without time
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 36:
        expected: select cast('12-2010-05' as timestamp format
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 37:
        expected:     'DD-YYYY-MM');
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 38:
        expected: EXPR$0
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 39:
        expected: 2010-05-12 00:00:00
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 40:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 41:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 42:
        expected: !if (fixed.calcite6375) {
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 43:
        expected: ### disabled until Bug.CALCITE_6375_FIXED ###
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 44:
        expected: 
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 45:
        expected: # Basic input to cover a datetime with timezone scenario
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 46:
        expected: select cast('2017-05-03 08:59:01.123456789PM 01:30'
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 47:
        expected:     as timestamp FORMAT 'YYYY-MM-DD HH12:MI:SS.FF9PM TZH:TZM');
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 48:
        expected: EXPR$0
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 49:
        expected: 2017-05-03 20:59:01.123456789
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: !ok
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 51:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 52:
        expected: # Shuffle the input timestamp and the format clause
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 53:
        expected: select cast('59 04-30-2017-05 01PM 01:08.123456789'
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 54:
        expected:     as timestamp FORMAT 'MI DD-TZM-YYYY-MM TZHPM SS:HH12.FF9');
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 55:
        expected: EXPR$0
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 56:
        expected: 2017-05-04 20:59:01.123456789
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 57:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 58:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 59:
        expected: # Input and format without separators
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 60:
        expected: # Note, 12:01 HH12 AM is 00:01 with the internal 0-23 representation.
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 61:
        expected: select cast('20170501120159123456789AM-0130' as
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 62:
        expected:     timestamp FORMAT 'YYYYDDMMHH12MISSFFAMTZHTZM');
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 63:
        expected: EXPR$0
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 64:
        expected: 2017-01-05 00:01:59.123456789
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 65:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 66:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 67:
        expected: # Shuffled input without separators
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 68:
        expected: select cast('59043020170501PM0108123456789'
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 69:
        expected:     as timestamp FORMAT 'MIDDTZMYYYYMMTZHPMSSHH12FF9');
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 70:
        expected: EXPR$0
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

cast.iq:
      line 29:
        expected: 
        actual:   Error while executing command ErrorCommand [sql: select 2 * CAST(5e18 AS DECIMAL(19, 0))]
      line 30:
        expected: select - CAST(-2147483648 AS INT);
        actual:   java.lang.RuntimeException: no connection
      line 31:
        expected: Caused by: java.lang.ArithmeticException
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:300)
      line 32:
        expected: !error
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 33:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 34:
        expected: select CAST(1 AS SMALLINT) + CAST(2 AS SMALLINT) AS C;
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 35:
        expected: +---+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 36:
        expected: | C |
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 37:
        expected: +---+
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 38:
        expected: | 3 |
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 39:
        expected: +---+
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 40:
        expected: (1 row)
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 41:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 42:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 43:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 44:
        expected: select CAST(127 AS TINYINT) + CAST(2 AS TINYINT) AS C;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 45:
        expected: Caused by: java.lang.ArithmeticException: integer overflow: Value 129 does not fit in a TINYINT
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 46:
        expected: !error
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 47:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 48:
        expected: select -2147483648 - 1;
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 49:
        expected: Caused by: java.lang.ArithmeticException
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 50:
        expected: !error
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 51:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 52:
        expected: select -2147483648 / -1;
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 53:
        expected: Caused by: java.lang.ArithmeticException
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 54:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 55:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 56:
        expected: select -CAST(-32768 AS SMALLINT);
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 57:
        expected: Caused by: java.lang.ArithmeticException: integer overflow: Value 32768 does not fit in a SMALLINT
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 58:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 59:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 60:
        expected: select CAST(32767 AS SMALLINT) + CAST(2 AS SMALLINT);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 61:
        expected: Caused by: java.lang.ArithmeticException: integer overflow: Value 32769 does not fit in a SMALLINT
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 62:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 63:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 64:
        expected: select 2147483647 * 2147483647;
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 65:
        expected: Caused by: java.lang.ArithmeticException
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 66:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 67:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 68:
        expected: !use scott
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)

conditions.iq:
      line 31:
        expected: 
        actual:   Error while executing command OkCommand [sql: WITH t(x) AS (VALUES(ROW(ROW(1)))) SELECT x IN (x) AS e FROM t]
      line 32:
        expected: WITH t(x) AS (VALUES(ROW(ROW(1)))) SELECT x NOT IN (x) AS e FROM t;
        actual:   java.lang.RuntimeException: no connection
      line 33:
        expected: +-------+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:300)
      line 34:
        expected: | E     |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 35:
        expected: +-------+
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 36:
        expected: | false |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 37:
        expected: +-------+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 38:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 39:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 40:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 41:
        expected: 
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 42:
        expected: WITH t(x) as (VALUES(ROW(ROW(4, 'cat')))) SELECT x IN (ROW(4, 'cat')) AS e FROM t;
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 43:
        expected: +------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 44:
        expected: | E    |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 45:
        expected: +------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 46:
        expected: | true |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 47:
        expected: +------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 48:
        expected: (1 row)
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 50:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 51:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 52:
        expected: # OR test
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 53:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 54:
        expected: with tmp(a, b) as (
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 55:
        expected:   values (1, 1), (1, 0), (1, cast(null as int))
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 56:
        expected:        , (0, 1), (0, 0), (0, cast(null as int))
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 57:
        expected:        , (cast(null as int), 1), (cast(null as int), 0), (cast(null as int), cast(null as int)))
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 58:
        expected: select *
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 59:
        expected:   from tmp
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 60:
        expected:  where a = 1 or b = 1
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 61:
        expected:  order by 1, 2;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 62:
        expected: +---+---+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 63:
        expected: | A | B |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 64:
        expected: +---+---+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 65:
        expected: | 0 | 1 |
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 66:
        expected: | 1 | 0 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 67:
        expected: | 1 | 1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 68:
        expected: | 1 |   |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 69:
        expected: |   | 1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 70:
        expected: +---+---+
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)

dual-table-query.iq:
      line 33:
        expected: 
        actual:   Error while executing command OkCommand [sql: SELECT 1 + 1 FROM DUAL]
      line 34:
        expected: SELECT 1 + 1;
        actual:   java.lang.RuntimeException: no connection
      line 35:
        expected: +--------+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:300)
      line 36:
        expected: | EXPR$0 |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 37:
        expected: +--------+
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 38:
        expected: |      2 |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 39:
        expected: +--------+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 40:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 41:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 42:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 43:
        expected: 
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 44:
        expected: # Oracle supports users to specify the dual table, but not supports users not to specify the dual table.
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 45:
        expected: !use scott-oracle
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 46:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 47:
        expected: SELECT 1 + 1 FROM DUAL;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 48:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 49:
        expected: | EXPR$0 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: +--------+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 51:
        expected: |      2 |
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 52:
        expected: +--------+
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 53:
        expected: (1 row)
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 54:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 55:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 56:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 57:
        expected: SELECT 1 + 1;
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 58:
        expected: java.sql.SQLException: Error while executing SQL "SELECT 1 + 1": From line 1, column 1 to line 1, column 12: SELECT must have a FROM clause
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 59:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 60:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 61:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 62:
        expected: SELECT * FROM DUAL;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 63:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 64:
        expected: | EXPR$0 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 65:
        expected: +--------+
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 66:
        expected: | X      |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 67:
        expected: +--------+
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 68:
        expected: (1 row)
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 69:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 70:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 71:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 72:
        expected: # End dual-table-query.iq
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)

functions.iq:
      line 33:
        expected: 
        actual:   Error while executing command OkCommand [sql: select bit_count(8)]
      line 34:
        expected: select bit_count('8');
        actual:   java.lang.RuntimeException: no connection
      line 35:
        expected: +--------+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:300)
      line 36:
        expected: | EXPR$0 |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 37:
        expected: +--------+
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 38:
        expected: |      1 |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 39:
        expected: +--------+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 40:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 41:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 42:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 43:
        expected: 
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 44:
        expected: select bit_count('a');
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 45:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 46:
        expected: | EXPR$0 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 47:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 48:
        expected: |      0 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 49:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: (1 row)
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 51:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 52:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 53:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 54:
        expected: select bit_count('');
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 55:
        expected: +--------+
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 56:
        expected: | EXPR$0 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 57:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 58:
        expected: |      0 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 59:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 60:
        expected: (1 row)
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 61:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 62:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 63:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 64:
        expected: select bit_count(null);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 65:
        expected: +--------+
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 66:
        expected: | EXPR$0 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 67:
        expected: +--------+
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 68:
        expected: |        |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 69:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 70:
        expected: (1 row)
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 71:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 72:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)

join.iq:
      line 29:
        expected: +---+----+----+----+
        actual:   java.sql.SQLException: Error executing query
      line 30:
        expected: | I | I0 | I1 | I2 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 31:
        expected: +---+----+----+----+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 32:
        expected: | 0 |  0 |  0 |  1 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 33:
        expected: | 0 |  0 |  1 |  0 |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 34:
        expected: | 0 |  1 |  0 |  0 |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 35:
        expected: | 1 |  0 |  0 |  0 |
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 36:
        expected: +---+----+----+----+
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 37:
        expected: (4 rows)
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 38:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 39:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 40:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 41:
        expected: with t (i) as (values (0), (1))
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 42:
        expected: select *
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 43:
        expected: from t as t1
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 44:
        expected:    cross join t as t2,
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 45:
        expected:  t as t3
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 46:
        expected: where t1.i + t2.i + t3.i = 1;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 47:
        expected: +---+----+----+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 48:
        expected: | I | I0 | I1 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: +---+----+----+
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 50:
        expected: | 0 |  0 |  1 |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 51:
        expected: | 0 |  1 |  0 |
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 52:
        expected: | 1 |  0 |  0 |
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 53:
        expected: +---+----+----+
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 54:
        expected: (3 rows)
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 55:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 56:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 57:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 58:
        expected: with t (i) as (values (0), (1))
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 59:
        expected: select *
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 60:
        expected: from t as t1,
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 61:
        expected:  t as t2
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 62:
        expected:    cross join t as t3
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 63:
        expected: where t1.i + t2.i + t3.i = 1;
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 64:
        expected: +---+----+----+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 65:
        expected: | I | I0 | I1 |
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 66:
        expected: +---+----+----+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 67:
        expected: | 0 |  0 |  1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 68:
        expected: | 0 |  1 |  0 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

lateral.iq:
      line 23:
        expected: parse failed: Encountered "join lateral \"scott\"" at line 1, column 27.
        actual:   java.sql.SQLException: Error executing query
      line 24:
        expected: Was expecting one of:
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 25:
        expected:     "AS" ...
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 26:
        expected:     "CROSS" ...
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 27:
        expected:     "EXTEND" ...
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 28:
        expected:     "FOR" ...
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 29:
        expected:     "OUTER" ...
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 30:
        expected:     "TABLESAMPLE" ...
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 31:
        expected: !error
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 32:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 33:
        expected: # Bad: LATERAL TABLE
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 34:
        expected: select * from "scott".emp join lateral table "scott".dept using (deptno);
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 35:
        expected: parse failed: Encountered "join lateral table \"scott\"" at line 1, column 27.
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 36:
        expected: Was expecting one of:
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 37:
        expected:     "AS" ...
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 38:
        expected:     "CROSS" ...
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 39:
        expected:     "EXTEND" ...
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 40:
        expected:     "FOR" ...
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 41:
        expected:     "OUTER" ...
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 42:
        expected:     "TABLESAMPLE" ...
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 43:
        expected: !error
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 44:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 45:
        expected: # Good: LATERAL (subQuery)
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 46:
        expected: # OK even as first item in FROM clause
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 47:
        expected: select * from lateral (select * from "scott".emp) where deptno = 10;
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 48:
        expected: +-------+--------+-----------+------+------------+---------+------+--------+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | SAL     | COMM | DEPTNO |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: +-------+--------+-----------+------+------------+---------+------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 51:
        expected: |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 | 2450.00 |      |     10 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 52:
        expected: |  7839 | KING   | PRESIDENT |      | 1981-11-17 | 5000.00 |      |     10 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 53:
        expected: |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 | 1300.00 |      |     10 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 54:
        expected: +-------+--------+-----------+------+------------+---------+------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 55:
        expected: (3 rows)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 56:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 57:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 58:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 59:
        expected: select * from lateral (select * from "scott".emp) as e where deptno = 10;
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 60:
        expected: +-------+--------+-----------+------+------------+---------+------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | SAL     | COMM | DEPTNO |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 62:
        expected: +-------+--------+-----------+------+------------+---------+------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

misc.iq:
      line 23:
        expected: SELECT "EMP"."ENAME", "EMP"."DEPTNO", "EMP"."GENDER"
        actual:   
      line 24:
        expected: FROM "POST"."EMP" AS "EMP"
        actual:   # [CALCITE-6751] Reduction of CAST from string to interval is incorrect
      line 25:
        expected: WHERE "EMP"."DEPTNO" > 20
        actual:   SELECT TIME '10:00:00' + CAST('1' AS INTERVAL SECOND);
      line 26:
        expected: !explain-validated-on Calcite
        actual:   java.sql.SQLException: Error executing query
      line 27:
        expected: 
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 28:
        expected: # [CALCITE-6751] Reduction of CAST from string to interval is incorrect
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 29:
        expected: SELECT TIME '10:00:00' + CAST('1' AS INTERVAL SECOND);
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 30:
        expected: +----------+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 31:
        expected: | EXPR$0   |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 32:
        expected: +----------+
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 33:
        expected: | 10:00:01 |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 34:
        expected: +----------+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 35:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 36:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 37:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 38:
        expected: 
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 39:
        expected: # Due to CALCITE-6752 the following test crashes:
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 40:
        expected: # SELECT TIME '10:00:00' + CAST('1.1' AS INTERVAL SECOND);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 41:
        expected: # +------------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 42:
        expected: # | EXPR$0     |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 43:
        expected: # +------------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 44:
        expected: # | 11:00:00.1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 45:
        expected: # +------------+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 46:
        expected: # (1 row)
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 47:
        expected: #
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 48:
        expected: # !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 49:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 50:
        expected: SELECT TIME '10:00:00' + CAST('1' AS INTERVAL HOUR);
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 51:
        expected: +----------+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 52:
        expected: | EXPR$0   |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 53:
        expected: +----------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 54:
        expected: | 11:00:00 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 55:
        expected: +----------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 56:
        expected: (1 row)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 57:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 58:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 59:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 60:
        expected: SELECT TIME '10:00:00' + CAST('1' AS INTERVAL MINUTE);
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 61:
        expected: +----------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 62:
        expected: | EXPR$0   |
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      ... (more differences truncated)

new-decorr.iq:
      line 36:
        expected: +-----+-----+
        actual:   java.sql.SQLException: Error executing query
      line 37:
        expected: | T0A | T0B |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 38:
        expected: +-----+-----+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 39:
        expected: |   2 |   0 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 40:
        expected: +-----+-----+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 41:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 42:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 43:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 44:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 45:
        expected: # [CALCITE-7356] The MARK JOIN generated by TopDownGeneralDecorrelator needs to be adapted to RelFieldTrimmer
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 46:
        expected: !use blank
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 47:
        expected: CREATE TABLE emps (
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 48:
        expected:   empid INTEGER NOT NULL,
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 49:
        expected:   deptno INTEGER NOT NULL,
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 50:
        expected:   name VARCHAR(10) NOT NULL,
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 51:
        expected:   salary DECIMAL(10, 2) NOT NULL,
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 52:
        expected:   commission INTEGER);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 53:
        expected: (0 rows modified)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 54:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 55:
        expected: !update
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 56:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 57:
        expected: INSERT INTO emps (empid, deptno, name, salary, commission) VALUES
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 58:
        expected: (100, 10, 'Bill', 10000.00, 1000),
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 59:
        expected: (200, 20, 'Eric', 8000.00, 500),
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 60:
        expected: (150, 10, 'Sebastian', 7000.00, NULL),
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 61:
        expected: (110, 10, 'Theodore', 11500.00, 250),
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 62:
        expected: (170, 30, 'Theodore', 11500.00, 250),
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 63:
        expected: (140, 10, 'Sebastian', 7000.00, NULL);
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 64:
        expected: (6 rows modified)
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 65:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 66:
        expected: !update
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 67:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 68:
        expected: SELECT empid, EXISTS(select * from (
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 69:
        expected:   SELECT e2.deptno FROM emps e2 where e1.commission = e2.commission) as table3
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 70:
        expected:   where table3.deptno <> e1.deptno)
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 71:
        expected: from emps e1 order by empid;
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 72:
        expected: +-------+--------+
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 73:
        expected: | EMPID | EXPR$1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 74:
        expected: +-------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 75:
        expected: |   100 | false  |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

operator.iq:
      line 22:
        expected: +------+
        actual:   java.sql.SQLException: Error executing query
      line 23:
        expected: | I    |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 24:
        expected: +------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 25:
        expected: | w    |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 26:
        expected: +------+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 27:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 28:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 29:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 30:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 31:
        expected: # [CALCITE-1095] NOT precedence
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 32:
        expected: select * from "scott".emp where not sal > 1300;
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 33:
        expected: +-------+--------+----------+------+------------+---------+---------+--------+
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 34:
        expected: | EMPNO | ENAME  | JOB      | MGR  | HIREDATE   | SAL     | COMM    | DEPTNO |
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 35:
        expected: +-------+--------+----------+------+------------+---------+---------+--------+
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 36:
        expected: |  7369 | SMITH  | CLERK    | 7902 | 1980-12-17 |  800.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 37:
        expected: |  7521 | WARD   | SALESMAN | 7698 | 1981-02-22 | 1250.00 |  500.00 |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 38:
        expected: |  7654 | MARTIN | SALESMAN | 7698 | 1981-09-28 | 1250.00 | 1400.00 |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 39:
        expected: |  7876 | ADAMS  | CLERK    | 7788 | 1987-05-23 | 1100.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 40:
        expected: |  7900 | JAMES  | CLERK    | 7698 | 1981-12-03 |  950.00 |         |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 41:
        expected: |  7934 | MILLER | CLERK    | 7782 | 1982-01-23 | 1300.00 |         |     10 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 42:
        expected: +-------+--------+----------+------+------------+---------+---------+--------+
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 43:
        expected: (6 rows)
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 44:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 45:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 46:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 47:
        expected: select count(*) as c from "scott".emp where not ename = 'SMITH';
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 48:
        expected: +----+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 49:
        expected: | C  |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 50:
        expected: +----+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 51:
        expected: | 13 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 52:
        expected: +----+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 53:
        expected: (1 row)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 54:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 55:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 56:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 57:
        expected: select count(*) as c from "scott".emp where not not ename = 'SMITH';
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 58:
        expected: +---+
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 59:
        expected: | C |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 60:
        expected: +---+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: | 1 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

outer.iq:
      line 22:
        expected: +-------+--------+--------+
        actual:   +--------+-------+--------+
      line 23:
        expected: | ENAME | DEPTNO | GENDER |
        actual:   | deptno | ename | gender |
      line 24:
        expected: +-------+--------+--------+
        actual:   +--------+-------+--------+
      line 25:
        expected: | Jane  |     10 | F      |
        actual:   |     10 | Bob   | M      |
      line 26:
        expected: | Bob   |     10 | M      |
        actual:   |     10 | Jane  | F      |
      line 27:
        expected: | Eric  |     20 | M      |
        actual:   |     20 | Eric  | M      |
      line 28:
        expected: | Susan |     30 | F      |
        actual:   |     30 | Alice | F      |
      line 29:
        expected: | Alice |     30 | F      |
        actual:   |     30 | Susan | F      |
      line 30:
        expected: | Adam  |     50 | M      |
        actual:   |     50 | Adam  | M      |
      line 31:
        expected: | Eve   |     50 | F      |
        actual:   |     50 | Eve   | F      |
      line 32:
        expected: | Grace |     60 | F      |
        actual:   |     60 | Grace | F      |
      line 33:
        expected: | Wilma |        | F      |
        actual:   |        | Wilma | F      |
      line 34:
        expected: +-------+--------+--------+
        actual:   +--------+-------+--------+
      line 39:
        expected: +-------+--------+--------+---------+-------------+
        actual:   java.sql.SQLException: Error executing query
      line 40:
        expected: | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 41:
        expected: +-------+--------+--------+---------+-------------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 42:
        expected: | Jane  |     10 | F      |      10 | Sales       |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 43:
        expected: | Bob   |     10 | M      |      10 | Sales       |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 44:
        expected: | Eric  |     20 | M      |      20 | Marketing   |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 45:
        expected: | Susan |     30 | F      |      30 | Engineering |
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 46:
        expected: | Alice |     30 | F      |      30 | Engineering |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 47:
        expected: +-------+--------+--------+---------+-------------+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 48:
        expected: (5 rows)
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 49:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 50:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 51:
        expected: 
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 52:
        expected: select * from emp join dept on emp.deptno = dept.deptno and emp.gender = 'F';
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 53:
        expected: +-------+--------+--------+---------+-------------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 54:
        expected: | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 55:
        expected: +-------+--------+--------+---------+-------------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 56:
        expected: | Alice |     30 | F      |      30 | Engineering |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 57:
        expected: | Jane  |     10 | F      |      10 | Sales       |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 58:
        expected: | Susan |     30 | F      |      30 | Engineering |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 59:
        expected: +-------+--------+--------+---------+-------------+
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 60:
        expected: (3 rows)
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 61:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 62:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 63:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 64:
        expected: select * from emp join dept on emp.deptno = dept.deptno where emp.gender = 'F';
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 65:
        expected: +-------+--------+--------+---------+-------------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

pivot.iq:
      line 29:
        expected: +--------+-----------------+---------------+-------------+-----------+-----------+---------+
        actual:   java.sql.SQLException: Error executing query
      line 30:
        expected: | DEPTNO | 'CLERK'_SUM_SAL | 'CLERK'_COUNT | MGR_SUM_SAL | MGR_COUNT | a_SUM_SAL | a_COUNT |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 31:
        expected: +--------+-----------------+---------------+-------------+-----------+-----------+---------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 32:
        expected: |     10 |         1300.00 |             1 |     2450.00 |         1 |           |       0 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 33:
        expected: |     20 |         1900.00 |             2 |     2975.00 |         1 |   6000.00 |       2 |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 34:
        expected: |     30 |          950.00 |             1 |     2850.00 |         1 |           |       0 |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 35:
        expected: +--------+-----------------+---------------+-------------+-----------+-----------+---------+
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 36:
        expected: (3 rows)
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 37:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 38:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 39:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 40:
        expected: # Oracle gives 'ORA-00918: column ambiguously defined'
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 41:
        expected: SELECT *
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 42:
        expected: FROM   (SELECT deptno, job, sal
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 43:
        expected:         FROM   emp)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 44:
        expected: PIVOT  (SUM(sal) AS sum_sal, COUNT(*) AS sal
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 45:
        expected:         FOR (job) IN ('CL' || 'ERK', 'MANAGER' mgr, 'ANALYST' AS "MGR_SUM",null))
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 46:
        expected: ORDER BY deptno;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 47:
        expected: At line 1, column 8: Column 'MGR_SUM_SAL' is ambiguous
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 48:
        expected: !error
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 50:
        expected: !if (false) {
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 51:
        expected: # Invalid column. (Because deptno is used in FOR, it is not available in ORDER BY.)
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 52:
        expected: SELECT *
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 53:
        expected: FROM (SELECT deptno, job, sal FROM emp)
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 54:
        expected: PIVOT (SUM(sal) AS sum_sal FOR (deptno,job) IN (10,'CLERK'))
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 55:
        expected: ORDER BY deptno;
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 56:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 57:
        expected: !}
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 58:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 59:
        expected: # Numeric axis without labels;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 60:
        expected: # note that 'SALESMAN' appears due to records in non-displayed departments.
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 61:
        expected: SELECT *
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 62:
        expected: FROM (SELECT job, deptno FROM emp)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 63:
        expected: PIVOT (COUNT(*) AS "COUNT" FOR deptno IN (10, 50, 20));
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 64:
        expected: +-----------+----------+----------+----------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 65:
        expected: | JOB       | 10_COUNT | 50_COUNT | 20_COUNT |
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 66:
        expected: +-----------+----------+----------+----------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 67:
        expected: | ANALYST   |        0 |        0 |        2 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 68:
        expected: | CLERK     |        1 |        0 |        2 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

qualify.iq:
      line 25:
        expected: EMPNO ENAME  DEPTNO
        actual:   java.sql.SQLException: Error executing query
      line 26:
        expected: ----- ------ ------
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 27:
        expected:  7369 SMITH      20
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 28:
        expected:  7499 ALLEN      30
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 29:
        expected:  7521 WARD       30
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 30:
        expected:  7566 JONES      20
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 31:
        expected:  7654 MARTIN     30
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 32:
        expected:  7698 BLAKE      30
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 33:
        expected:  7782 CLARK      10
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 34:
        expected:  7788 SCOTT      20
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 35:
        expected:  7839 KING       10
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 36:
        expected:  7844 TURNER     30
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 37:
        expected:  7876 ADAMS      20
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 38:
        expected:  7900 JAMES      30
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 39:
        expected:  7902 FORD       20
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 40:
        expected:  7934 MILLER     10
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 41:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 42:
        expected: 14 rows selected.
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 43:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 44:
        expected: !ok
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 45:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 46:
        expected: # Test QUALIFY without any references but with regular filter
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 47:
        expected: SELECT empno, ename, deptno
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 48:
        expected: FROM "scott".emp
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 49:
        expected: WHERE deptno > 20
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 50:
        expected: QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1;
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 51:
        expected: EMPNO ENAME  DEPTNO
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 52:
        expected: ----- ------ ------
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 53:
        expected:  7499 ALLEN      30
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 54:
        expected:  7521 WARD       30
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 55:
        expected:  7654 MARTIN     30
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 56:
        expected:  7698 BLAKE      30
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 57:
        expected:  7844 TURNER     30
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 58:
        expected:  7900 JAMES      30
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 59:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 60:
        expected: 6 rows selected.
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 61:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 62:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 63:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 64:
        expected: # Test QUALIFY with references
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

recursive_queries.iq:
      line 29:
        expected: +---------+
        actual:   java.sql.SQLException: Error executing query
      line 30:
        expected: | A       |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 31:
        expected: +---------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 32:
        expected: | 3628800 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 33:
        expected: +---------+
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 34:
        expected: (1 row)
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 35:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 36:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 37:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 38:
        expected: WITH RECURSIVE FibonacciCTE(n, a, b) AS (
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 39:
        expected:     SELECT 1, 0, 1
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 40:
        expected:     UNION ALL
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 41:
        expected:     SELECT n + 1, b, a + b
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 42:
        expected:     FROM FibonacciCTE
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 43:
        expected:     WHERE n < 10
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 44:
        expected: )
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 45:
        expected: SELECT a FROM FibonacciCTE;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 46:
        expected: +----+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 47:
        expected: | A  |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 48:
        expected: +----+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: |  0 |
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 50:
        expected: |  1 |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 51:
        expected: |  1 |
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 52:
        expected: | 13 |
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 53:
        expected: |  2 |
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 54:
        expected: | 21 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 55:
        expected: |  3 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 56:
        expected: | 34 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 57:
        expected: |  5 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 58:
        expected: |  8 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 59:
        expected: +----+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 60:
        expected: (10 rows)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 61:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 62:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 63:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 64:
        expected: WITH RECURSIVE CumulativeSumCTE(n, a) AS (
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 65:
        expected:     SELECT 1, 10
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 66:
        expected:     UNION ALL
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 67:
        expected:     SELECT n + 1, a + (n + 1)
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 68:
        expected:     FROM CumulativeSumCTE
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

scalar.iq:
      line 23:
        expected: java.lang.ArithmeticException: integer overflow
        actual:   java.sql.SQLException: Error executing query
      line 24:
        expected: 
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 25:
        expected: !error
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 26:
        expected: 
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 27:
        expected: SELECT INTERVAL 2147483647 years;
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 28:
        expected: java.lang.ArithmeticException: integer overflow
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 29:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 30:
        expected: !error
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 31:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 32:
        expected: SELECT -(INTERVAL -9223372036854775.808 SECONDS);
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 33:
        expected: java.lang.ArithmeticException: long overflow
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 34:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 35:
        expected: !error
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 36:
        expected: 
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 37:
        expected: SELECT INTERVAL 3000000 months * 1000;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 38:
        expected: java.lang.ArithmeticException: integer overflow
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 39:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 40:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 41:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 42:
        expected: SELECT INTERVAL 3000000 months / .0001;
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 43:
        expected: java.lang.ArithmeticException: Overflow
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 44:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 45:
        expected: !error
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 46:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 47:
        expected: select deptno, (select min(empno) from "scott".emp where deptno = dept.deptno) as x from "scott".dept;
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 48:
        expected: +--------+------+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: | DEPTNO | X    |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: +--------+------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 51:
        expected: |     10 | 7782 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 52:
        expected: |     20 | 7369 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 53:
        expected: |     30 | 7499 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 54:
        expected: |     40 |      |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 55:
        expected: +--------+------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 56:
        expected: (4 rows)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 57:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 58:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 59:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 60:
        expected: select deptno, (select count(*) from "scott".emp where deptno = dept.deptno) as x from "scott".dept;
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: +--------+---+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 62:
        expected: | DEPTNO | X |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

sequence.iq:
      line 32:
        expected: select current value for "my_seq" as c from (values 1, 2);
        actual:   Error while executing command OkCommand [sql: select next value for "my_seq" as c from (values 1, 2)]
      line 33:
        expected: +---+
        actual:   java.lang.RuntimeException: no connection
      line 34:
        expected: | C |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:300)
      line 35:
        expected: +---+
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 36:
        expected: | 2 |
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 37:
        expected: | 2 |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 38:
        expected: +---+
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 39:
        expected: (2 rows)
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 40:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 41:
        expected: !ok
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 42:
        expected: 
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 43:
        expected: select next value for "my_seq" as c from (values 1, 2);
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 44:
        expected: C BIGINT(19) NOT NULL
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 45:
        expected: !type
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 46:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 47:
        expected: # Qualified with schema name
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 48:
        expected: select next value for "s"."my_seq" as c from (values 1, 2);
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 49:
        expected: C BIGINT(19) NOT NULL
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 50:
        expected: !type
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 51:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 52:
        expected: select next value for "unknown_seq" as c from (values 1, 2);
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 53:
        expected: From line 1, column 23 to line 1, column 35: Table 'unknown_seq' not found
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 54:
        expected: !error
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 55:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 56:
        expected: # Qualified with bad schema name
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 57:
        expected: select next value for "unknown_schema"."my_seq" as c from (values 1, 2);
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 58:
        expected: From line 1, column 23 to line 1, column 47: Table 'unknown_schema.my_seq' not found
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 59:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 60:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 61:
        expected: # Table found, but not a sequence
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 62:
        expected: select next value for "metadata".tables as c from (values 1, 2);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 63:
        expected: From line 1, column 23 to line 1, column 39: Table 'metadata.TABLES' is not a sequence
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 64:
        expected: !error
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 65:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 66:
        expected: # Sequences appear in the catalog as tables of type 'SEQUENCE'
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 67:
        expected: select * from "metadata".tables;
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 68:
        expected: +----------+------------+-----------+--------------+---------+---------+-----------+----------+------------------------+---------------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 69:
        expected: | tableCat | tableSchem | tableName | tableType    | remarks | typeCat | typeSchem | typeName | selfReferencingColName | refGeneration |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 70:
        expected: +----------+------------+-----------+--------------+---------+---------+-----------+----------+------------------------+---------------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 71:
        expected: |          | metadata   | COLUMNS   | SYSTEM TABLE |         |         |           |          |                        |               |
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)

set-op.iq:
      line 26:
        expected: +---+---+
        actual:   java.sql.SQLException: Error executing query
      line 27:
        expected: | X | Y |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 28:
        expected: +---+---+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 29:
        expected: | 1 | a |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 30:
        expected: | 1 | a |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 31:
        expected: +---+---+
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 32:
        expected: (2 rows)
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 33:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 34:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 35:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 36:
        expected: # Intersect
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 37:
        expected: select * from
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 38:
        expected: (select x, y from (values (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 39:
        expected: intersect
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 40:
        expected: (select x, y from (values (1, 'a'), (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 41:
        expected: +---+---+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 42:
        expected: | X | Y |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 43:
        expected: +---+---+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 44:
        expected: | 1 | a |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 45:
        expected: +---+---+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 46:
        expected: (1 row)
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 47:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 48:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 49:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 50:
        expected: # Intersect all with null value rows
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 51:
        expected: select * from
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 52:
        expected: (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 53:
        expected:   (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 54:
        expected: intersect all
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 55:
        expected: (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 56:
        expected:   (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 57:
        expected: +---+---+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 58:
        expected: | X | Y |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 59:
        expected: +---+---+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 60:
        expected: |   |   |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 61:
        expected: |   |   |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 62:
        expected: +---+---+
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 63:
        expected: (2 rows)
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 64:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 65:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

some.iq:
      line 24:
        expected: +-------+--------+-----------+------+------------+---------+---------+--------+
        actual:   java.sql.SQLException: Error executing query
      line 25:
        expected: | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | SAL     | COMM    | DEPTNO |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 26:
        expected: +-------+--------+-----------+------+------------+---------+---------+--------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 27:
        expected: |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |  800.00 |         |     20 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 28:
        expected: |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 | 1600.00 |  300.00 |     30 |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 29:
        expected: |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 | 1250.00 |  500.00 |     30 |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 30:
        expected: |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 | 2975.00 |         |     20 |
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 31:
        expected: |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 | 1250.00 | 1400.00 |     30 |
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 32:
        expected: |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 | 2850.00 |         |     30 |
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 33:
        expected: |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 | 2450.00 |         |     10 |
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 34:
        expected: |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 | 3000.00 |         |     20 |
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 35:
        expected: |  7839 | KING   | PRESIDENT |      | 1981-11-17 | 5000.00 |         |     10 |
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 36:
        expected: |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 | 1500.00 |    0.00 |     30 |
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 37:
        expected: |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 | 1100.00 |         |     20 |
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 38:
        expected: |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |  950.00 |         |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 39:
        expected: |  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 | 3000.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 40:
        expected: |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 | 1300.00 |         |     10 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 41:
        expected: +-------+--------+-----------+------+------------+---------+---------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 42:
        expected: (14 rows)
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 43:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 44:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 45:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 46:
        expected: # Both sides NOT NULL
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 47:
        expected: select * from "scott".emp
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 48:
        expected: where empno > any (select deptno from "scott".dept);
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 49:
        expected: +-------+--------+-----------+------+------------+---------+---------+--------+
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 50:
        expected: | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | SAL     | COMM    | DEPTNO |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 51:
        expected: +-------+--------+-----------+------+------------+---------+---------+--------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 52:
        expected: |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |  800.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 53:
        expected: |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 | 1600.00 |  300.00 |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 54:
        expected: |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 | 1250.00 |  500.00 |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 55:
        expected: |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 | 2975.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 56:
        expected: |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 | 1250.00 | 1400.00 |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 57:
        expected: |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 | 2850.00 |         |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 58:
        expected: |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 | 2450.00 |         |     10 |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 59:
        expected: |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 | 3000.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 60:
        expected: |  7839 | KING   | PRESIDENT |      | 1981-11-17 | 5000.00 |         |     10 |
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 61:
        expected: |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 | 1500.00 |    0.00 |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 62:
        expected: |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 | 1100.00 |         |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 63:
        expected: |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |  950.00 |         |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

sort.iq:
      line 24:
        expected: EnumerableTableScan(table=[[foodmart2, days]])
        actual:   Error while executing command VerifyCommand [sql: select * from "days" order by "day"]
      line 25:
        expected: !plan
        actual:   java.lang.RuntimeException: no connection
      line 26:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:300)
      line 27:
        expected: # The ArrayTable "days" is sorted by "day", so the plan does not sort, only applies limit
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 28:
        expected: select * from "days" order by "day" limit 2;
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 29:
        expected: +-----+----------+
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 30:
        expected: | day | week_day |
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 31:
        expected: +-----+----------+
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 32:
        expected: |   1 | Sunday   |
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 33:
        expected: |   2 | Monday   |
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 34:
        expected: +-----+----------+
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 35:
        expected: (2 rows)
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 36:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 37:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 38:
        expected: EnumerableLimit(fetch=[2])
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 39:
        expected:   EnumerableTableScan(table=[[foodmart2, days]])
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 40:
        expected: !plan
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 41:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 42:
        expected: # The ArrayTable "days" is sorted by "day", so the plan must not contain Sort
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 43:
        expected: select * from "days" where "day" between 2 and 4 order by "day";
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 44:
        expected: +-----+-----------+
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 45:
        expected: | day | week_day  |
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 46:
        expected: +-----+-----------+
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 47:
        expected: |   2 | Monday    |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 48:
        expected: |   3 | Tuesday   |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 49:
        expected: |   4 | Wednesday |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 50:
        expected: +-----+-----------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 51:
        expected: (3 rows)
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 52:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 53:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 54:
        expected: EnumerableCalc(expr#0..1=[{inputs}], expr#2=[Sarg[[2..4]]], expr#3=[SEARCH($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 55:
        expected:   EnumerableTableScan(table=[[foodmart2, days]])
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 56:
        expected: !plan
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 57:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 58:
        expected: # [CALCITE-970] Default collation of NULL values
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 59:
        expected: # Nulls high, i.e. first if DESC
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 60:
        expected: select "store_id", "grocery_sqft" from "store"
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: where "store_id" < 3
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 62:
        expected: order by 2 DESC;
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 63:
        expected: +----------+--------------+
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)

sub-query.iq:
      line 30:
        expected: +---+
        actual:   java.sql.SQLException: Error executing query
      line 31:
        expected: | X |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 32:
        expected: +---+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 33:
        expected: +---+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 34:
        expected: (0 rows)
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 35:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 36:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 37:
        expected: !if (use_old_decorr) {
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 38:
        expected: EnumerableCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[=($t1, $t5)], expr#7=[IS NULL($t4)], expr#8=[>=($t2, $t1)], expr#9=[IS NOT NULL($t0)], expr#10=[AND($t7, $t8, $t9)], expr#11=[OR($t6, $t10)], X=[$t0], $condition=[$t11])
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 39:
        expected:   EnumerableMergeJoin(condition=[=($0, $3)], joinType=[left])
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 40:
        expected:     EnumerableNestedLoopJoin(condition=[true], joinType=[inner])
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 41:
        expected:       EnumerableValues(tuples=[[{ 1 }, { 2 }, { null }]])
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 42:
        expected:       EnumerableAggregate(group=[{}], c=[COUNT()], ck=[COUNT($0)])
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 43:
        expected:         EnumerableValues(tuples=[[{ 1 }, { null }]])
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 44:
        expected:     EnumerableCalc(expr#0=[{inputs}], expr#1=[true], proj#0..1=[{exprs}])
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 45:
        expected:       EnumerableValues(tuples=[[{ 1 }, { null }]])
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 46:
        expected: !plan
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 47:
        expected: !}
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 48:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 49:
        expected: # Use of case is to get around issue with directly specifying null in values
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 50:
        expected: # list. Postgres gives 0 rows.
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 51:
        expected: with
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 52:
        expected: t1(x) as (select * from  (values (1),(2),(case when 1 = 1 then null else 3 end)) as t1),
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 53:
        expected: t2(x) as (select * from  (values (1),(case when 1 = 1 then null else 3 end)) as t2)
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 54:
        expected: select *
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 55:
        expected: from t1
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 56:
        expected: where t1.x not in (select t2.x from t2);
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 57:
        expected: +---+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 58:
        expected: | X |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 59:
        expected: +---+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 60:
        expected: +---+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 61:
        expected: (0 rows)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 62:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 63:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 64:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 65:
        expected: # RHS has a mixture of NULL and NOT NULL keys
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 66:
        expected: select * from dept where deptno not in (select deptno from emp);
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 67:
        expected: +--------+-------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 68:
        expected: | DEPTNO | DNAME |
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 69:
        expected: +--------+-------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

tablesample.iq:
      line 23:
        expected: +------+
        actual:   java.sql.SQLException: Error executing query
      line 24:
        expected: | COMM |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 25:
        expected: +------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 26:
        expected: +------+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 27:
        expected: (0 rows)
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 28:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 29:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 30:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 31:
        expected: # Should always return all rows
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 32:
        expected: select deptno from "scott".dept tablesample system(100);
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 33:
        expected: +--------+
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 34:
        expected: | DEPTNO |
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 35:
        expected: +--------+
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 36:
        expected: |     10 |
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 37:
        expected: |     20 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 38:
        expected: |     30 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 39:
        expected: |     40 |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 40:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 41:
        expected: (4 rows)
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 42:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 43:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 44:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 45:
        expected: # End tablesample.iq
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 46:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 47:
        expected: <EOF>
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 48:
        expected: <EOF>
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 51:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 52:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 53:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 54:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 55:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 56:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 57:
        expected: <EOF>
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 58:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 59:
        expected: <EOF>
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 60:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 62:
        expected: <EOF>
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

unnest.iq:
      line 23:
        expected: +---+---+
        actual:   java.sql.SQLException: Error executing query
      line 24:
        expected: | X | Y |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 25:
        expected: +---+---+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 26:
        expected: | 1 | a |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 27:
        expected: | 2 | b |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 28:
        expected: +---+---+
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 29:
        expected: (2 rows)
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 30:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 31:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 32:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 33:
        expected: select *
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 34:
        expected: from UNNEST(array ['apple', 'banana']) as fruit (fruit);
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 35:
        expected: +--------+
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 36:
        expected: | FRUIT  |
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 37:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 38:
        expected: | apple  |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 39:
        expected: | banana |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 40:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 41:
        expected: (2 rows)
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 42:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 43:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 44:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 45:
        expected: # When UNNEST produces a single column, and you use an alias for the
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 46:
        expected: # relation, that alias becomes the name of the column.
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 47:
        expected: select *
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 48:
        expected: from UNNEST(array ['apple', 'banana']) as fruit;
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: | FRUIT  |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 51:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 52:
        expected: | apple  |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 53:
        expected: | banana |
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 54:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 55:
        expected: (2 rows)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 56:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 57:
        expected: !ok
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 58:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 59:
        expected: select fruit.*
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 60:
        expected: from UNNEST(array ['apple', 'banana']) as fruit;
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: +--------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 62:
        expected: | FRUIT  |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

unsigned.iq:
      line 23:
        expected: EXPR$0
        actual:   java.sql.SQLException: Error executing query
      line 24:
        expected: 6
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 25:
        expected: !ok
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 26:
        expected: 
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 27:
        expected: SELECT -CAST(200 AS INT UNSIGNED);
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 28:
        expected: java.sql.SQLException: Error while executing SQL "SELECT -CAST(200 AS INT UNSIGNED)": From line 1, column 8 to line 1, column 33: Cannot apply '-' to arguments of type '-<INTEGER UNSIGNED>'. Supported form(s): '-<INTEGER>'
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 29:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 30:
        expected: !error
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 31:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 32:
        expected: SELECT CAST(200 AS INT UNSIGNED) - 100;
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 33:
        expected: EXPR$0
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 34:
        expected: 100
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 35:
        expected: !ok
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 36:
        expected: 
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 37:
        expected: SELECT CAST(1 AS INT UNSIGNED);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 38:
        expected: EXPR$0
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 39:
        expected: 1
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 40:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 41:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 42:
        expected: SELECT CAST(-1 AS INT UNSIGNED);
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 43:
        expected: java.lang.NumberFormatException: Value is out of range : -1
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 44:
        expected: !error
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 45:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 46:
        expected: SELECT CAST(255 AS TINYINT UNSIGNED);
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 47:
        expected: EXPR$0
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 48:
        expected: 255
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 49:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 50:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 51:
        expected: SELECT CAST(255 AS TINYINT);
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 52:
        expected: java.lang.ArithmeticException: Value 255 out of range
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 53:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 54:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 55:
        expected: SELECT CAST(200 AS INT UNSIGNED) - CAST(100 AS INT UNSIGNED);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 56:
        expected: EXPR$0
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 57:
        expected: 100
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 58:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 59:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 60:
        expected: SELECT CAST(100 AS INT UNSIGNED) - CAST(200 AS INT UNSIGNED);
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 61:
        expected: java.lang.NumberFormatException: Value is out of range : -100
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 62:
        expected: !error
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

winagg.iq:
      line 34:
        expected: +------+----+---+
        actual:   java.sql.SQLException: Error executing query
      line 35:
        expected: | T    | L  | C |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryRequest(StatementImpl.java:76)
      line 36:
        expected: +------+----+---+
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQueryX(StatementImpl.java:53)
      line 37:
        expected: | +10  | 10 | 1 |
        actual:   	at org.opensearch.jdbc.StatementImpl.executeQuery(StatementImpl.java:46)
      line 38:
        expected: | -28  | 10 | 0 |
        actual:   	at net.hydromatic.quidem.Quidem.checkResult(Quidem.java:317)
      line 39:
        expected: | +754 | 10 | 0 |
        actual:   	at net.hydromatic.quidem.Quidem.access$2600(Quidem.java:54)
      line 40:
        expected: +------+----+---+
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.checkResult(Quidem.java:1778)
      line 41:
        expected: (3 rows)
        actual:   	at net.hydromatic.quidem.Quidem$CheckResultCommand.execute(Quidem.java:985)
      line 42:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 43:
        expected: !ok
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 44:
        expected: 
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 45:
        expected: # Multiple window functions sharing a single window
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 46:
        expected: select count(*) over(partition by gender order by ename) as count1,
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 47:
        expected:   count(*) over(partition by deptno order by ename) as count2,
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 48:
        expected:   sum(deptno) over(partition by gender order by ename) as sum1,
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 49:
        expected:   sum(deptno) over(partition by deptno order by ename) as sum2
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 50:
        expected: from emp
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 51:
        expected: order by sum1, sum2;
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 52:
        expected: +--------+--------+------+------+
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 53:
        expected: | COUNT1 | COUNT2 | SUM1 | SUM2 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 54:
        expected: +--------+--------+------+------+
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 55:
        expected: |      1 |      1 |   30 |   30 |
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 56:
        expected: |      1 |      1 |   50 |   50 |
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 57:
        expected: |      2 |      1 |   60 |   10 |
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 58:
        expected: |      3 |      1 |   80 |   20 |
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 59:
        expected: |      2 |      2 |   80 |  100 |
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 60:
        expected: |      3 |      1 |  140 |   60 |
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 61:
        expected: |      4 |      2 |  150 |   20 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 62:
        expected: |      5 |      2 |  180 |   60 |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 63:
        expected: |      6 |      1 |  180 |      |
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 64:
        expected: +--------+--------+------+------+
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 65:
        expected: (9 rows)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 66:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 67:
        expected: !ok
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 68:
        expected: 
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 69:
        expected: !use scott
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 70:
        expected: # Check default brackets. Note that:
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 71:
        expected: # c2 and c3 are equivalent to c1;
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 72:
        expected: # c5 is equivalent to c4;
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 73:
        expected: # c7 is equivalent to c6.
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      ... (more differences truncated)

within-distinct.iq:
      line 86:
        expected: (0 rows modified)
        actual:   java.sql.SQLFeatureNotSupportedException: Updates are not supported.
      line 87:
        expected: 
        actual:   	at org.opensearch.jdbc.StatementImpl.executeUpdate(StatementImpl.java:88)
      line 88:
        expected: !update
        actual:   	at net.hydromatic.quidem.Quidem.update(Quidem.java:268)
      line 89:
        expected: 
        actual:   	at net.hydromatic.quidem.Quidem.access$2700(Quidem.java:54)
      line 90:
        expected: CREATE TABLE dept AS
        actual:   	at net.hydromatic.quidem.Quidem$ContextImpl.update(Quidem.java:1787)
      line 91:
        expected: SELECT * FROM (VALUES
        actual:   	at net.hydromatic.quidem.Quidem$UpdateCommand.execute(Quidem.java:1090)
      line 92:
        expected:  (10, 'ACCOUNTING', 'NEW YORK'),
        actual:   	at net.hydromatic.quidem.Quidem$CompositeCommand.execute(Quidem.java:1522)
      line 93:
        expected:  (20, 'RESEARCH',   'DALLAS'),
        actual:   	at net.hydromatic.quidem.Quidem.execute(Quidem.java:204)
      line 94:
        expected:  (30, 'SALES',      'CHICAGO'),
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runOne(AnsiSqlQuidemIT.java:139)
      line 95:
        expected:  (40, 'OPERATIONS', 'BOSTON')
        actual:   	at org.opensearch.sql.ansi.AnsiSqlQuidemIT.runAllIqFiles(AnsiSqlQuidemIT.java:84)
      line 96:
        expected: ) AS dept (DEPTNO, DNAME, LOC);
        actual:   	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
      line 97:
        expected: (0 rows modified)
        actual:   	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
      line 98:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.invoke(RandomizedRunner.java:1750)
      line 99:
        expected: !update
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$8.evaluate(RandomizedRunner.java:938)
      line 100:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$9.evaluate(RandomizedRunner.java:974)
      line 101:
        expected: CREATE TABLE bonus (
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$10.evaluate(RandomizedRunner.java:988)
      line 102:
        expected:   ENAME VARCHAR(10),
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 103:
        expected:   JOB  VARCHAR(9),
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 104:
        expected:   SAL  DECIMAL(6, 2),
        actual:   	at org.apache.lucene.tests.util.TestRuleSetupTeardownChained$1.evaluate(TestRuleSetupTeardownChained.java:48)
      line 105:
        expected:   COMM DECIMAL(6, 2));
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 106:
        expected: (0 rows modified)
        actual:   	at org.apache.lucene.tests.util.TestRuleThreadAndTestName$1.evaluate(TestRuleThreadAndTestName.java:45)
      line 107:
        expected: 
        actual:   	at org.apache.lucene.tests.util.TestRuleIgnoreAfterMaxFailures$1.evaluate(TestRuleIgnoreAfterMaxFailures.java:60)
      line 108:
        expected: !update
        actual:   	at org.apache.lucene.tests.util.TestRuleMarkFailure$1.evaluate(TestRuleMarkFailure.java:44)
      line 109:
        expected: 
        actual:   	at org.junit.rules.RunRules.evaluate(RunRules.java:20)
      line 110:
        expected: CREATE TABLE salgrade AS
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 111:
        expected: SELECT * FROM (VALUES
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$StatementRunner.run(ThreadLeakControl.java:368)
      line 112:
        expected:   (1,  700, 1200),
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl.forkTimeoutingTask(ThreadLeakControl.java:817)
      line 113:
        expected:   (2, 1201, 1400),
        actual:   	at com.carrotsearch.randomizedtesting.ThreadLeakControl$3.evaluate(ThreadLeakControl.java:468)
      line 114:
        expected:   (3, 1401, 2000),
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner.runSingleTest(RandomizedRunner.java:947)
      line 115:
        expected:   (4, 2001, 3000),
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$5.evaluate(RandomizedRunner.java:832)
      line 116:
        expected:   (5, 3001, 9999)
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$6.evaluate(RandomizedRunner.java:883)
      line 117:
        expected: ) AS salgrade (GRADE, LOSAL, HISAL);
        actual:   	at com.carrotsearch.randomizedtesting.RandomizedRunner$7.evaluate(RandomizedRunner.java:894)
      line 118:
        expected: (0 rows modified)
        actual:   	at org.apache.lucene.tests.util.AbstractBeforeAfterRule$1.evaluate(AbstractBeforeAfterRule.java:43)
      line 119:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 120:
        expected: !update
        actual:   	at org.apache.lucene.tests.util.TestRuleStoreClassName$1.evaluate(TestRuleStoreClassName.java:38)
      line 121:
        expected: 
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 122:
        expected: CREATE TABLE dummy AS
        actual:   	at com.carrotsearch.randomizedtesting.rules.NoShadowingOrOverridesOnMethodsRule$1.evaluate(NoShadowingOrOverridesOnMethodsRule.java:40)
      line 123:
        expected: SELECT * FROM (VALUES
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 124:
        expected:   (0)
        actual:   	at com.carrotsearch.randomizedtesting.rules.StatementAdapter.evaluate(StatementAdapter.java:36)
      line 125:
        expected: ) AS dummy (DUMMY);
        actual:   	at org.apache.lucene.tests.util.TestRuleAssertionsRequired$1.evaluate(TestRuleAssertionsRequired.java:53)
      ... (more differences truncated)
