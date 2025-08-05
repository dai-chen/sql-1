## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Enhancements
* Build: Cache Prometheus binary ([#3261](https://github.com/opensearch-project/sql/pull/3261))
* Decimal literal should convert to double in pushdown ([#3811](https://github.com/opensearch-project/sql/pull/3811))
* Support Sort pushdown ([#3620](https://github.com/opensearch-project/sql/pull/3620))
* Support span push down ([#3823](https://github.com/opensearch-project/sql/pull/3823))
* Change compare logical when comparing date related fields with string literal ([#3798](https://github.com/opensearch-project/sql/pull/3798))
* Support relevance query functions pushdown implementation in Calcite ([#3834](https://github.com/opensearch-project/sql/pull/3834))
* Support filter push down for Sarg value ([#3840](https://github.com/opensearch-project/sql/pull/3840))
* Add big5 to IT Suite ([#3822](https://github.com/opensearch-project/sql/pull/3822))
* Skipping codegen and compile for Scan only plan ([#3853](https://github.com/opensearch-project/sql/pull/3853))
* Add ClickBench IT Suite ([#3860](https://github.com/opensearch-project/sql/pull/3860))
* Add compare_ip operator udfs ([#3821](https://github.com/opensearch-project/sql/pull/3821))
* Support pushdown physical sort operator to speedup SortMergeJoin ([#3864](https://github.com/opensearch-project/sql/pull/3864))
* Push down QUERY_SIZE_LIMIT ([#3880](https://github.com/opensearch-project/sql/pull/3880))
* Support partial filter push down ([#3850](https://github.com/opensearch-project/sql/pull/3850))
* Support full expression in WHERE clauses ([#3849](https://github.com/opensearch-project/sql/pull/3849))
* CVE-2025-48924: upgrade commons-lang3 to 3.18.0 ([#3895](https://github.com/opensearch-project/sql/pull/3895))
* Filter script pushdown with RelJson serialization in Calcite ([#3859](https://github.com/opensearch-project/sql/pull/3859))
* Support aggregation push down with scripts ([#3916](https://github.com/opensearch-project/sql/pull/3916))
* Support casting to IP with Calcite ([#3919](https://github.com/opensearch-project/sql/pull/3919))
* Support function argument coercion with Calcite ([#3914](https://github.com/opensearch-project/sql/pull/3914))
* Append limit operator for QUEERY_SIZE_LIMIT ([#3940](https://github.com/opensearch-project/sql/pull/3940))
* Disable a failed PPL query fallback to v2 by default ([#3952](https://github.com/opensearch-project/sql/pull/3952))

### Bug Fixes
* Fix flaky tests related to WeekOfYear in CalcitePPLDateTimeBuiltinFunctionIT ([#3815](https://github.com/opensearch-project/sql/pull/3815))
* Correct null order for `sort` command with Calcite ([#3835](https://github.com/opensearch-project/sql/pull/3835))
* Translate JSONException to 400 instead of 500 ([#3833](https://github.com/opensearch-project/sql/pull/3833))
* Fix relevance query function over optimization issue in ReduceExpressionsRule ([#3851](https://github.com/opensearch-project/sql/pull/3851))
* Support struct field with dynamic disabled ([#3829](https://github.com/opensearch-project/sql/pull/3829))
* Allow warning header for yaml test ([#3846](https://github.com/opensearch-project/sql/pull/3846))
* Fix incorrect push down for Sarg with nullAs is TRUE ([#3882](https://github.com/opensearch-project/sql/pull/3882))
* Fix the count() only aggregation pushdown issue ([#3891](https://github.com/opensearch-project/sql/pull/3891))
* Fix flaky tests in `RestHandlerClientYamlTestSuiteIT` ([#3901](https://github.com/opensearch-project/sql/pull/3901))
* Support casting date literal to timestamp ([#3831](https://github.com/opensearch-project/sql/pull/3831))
* Default to UTC for date/time functions across PPL and SQL ([#3854](https://github.com/opensearch-project/sql/pull/3854))
* Byte number should treated as Long in doc values ([#3928](https://github.com/opensearch-project/sql/pull/3928))
* Fix create PIT permissions issue ([#3921](https://github.com/opensearch-project/sql/pull/3921))
* Convert like function call to wildcard query for Calcite filter pushdown ([#3915](https://github.com/opensearch-project/sql/pull/3915))
* Increase the precision of sum return type ([#3974](https://github.com/opensearch-project/sql/pull/3974))

### Infrastructure
* Bump gradle to 8.14 and java to 24 ([#3875](https://github.com/opensearch-project/sql/pull/3875))
* Add 'testing' and 'security fix' to enforce-label-action ([#3897](https://github.com/opensearch-project/sql/pull/3897))
* Update the maven snapshot publish endpoint and credential ([#3806](https://github.com/opensearch-project/sql/pull/3806))

### Documentation
* Add debugger details to dev guide ([#3732](https://github.com/opensearch-project/sql/pull/3732))
* Update the limitation docs ([#3801](https://github.com/opensearch-project/sql/pull/3801))
* Update ppl documentation index for new functions ([#3868](https://github.com/opensearch-project/sql/pull/3868))
* Add missing command in index.rst ([#3943](https://github.com/opensearch-project/sql/pull/3943))
* Add issue template specific for PPL commands and queries ([#3962](https://github.com/opensearch-project/sql/pull/3962))
* Add debugger details to dev guide ([#3732](https://github.com/opensearch-project/sql/pull/3732))

### Maintenance
* Migrate standalone integration tests to remote tests ([#3778](https://github.com/opensearch-project/sql/pull/3778))
* Resolve commons beanutils to 2.0 ([#3787](https://github.com/opensearch-project/sql/pull/3787))
* Remove unneeded dependency on commons-validator ([#3799](https://github.com/opensearch-project/sql/pull/3799))
* Remove aviator dep ([#3805](https://github.com/opensearch-project/sql/pull/3805))
* Add Commit-Based Artifact Download Support ([#3736](https://github.com/opensearch-project/sql/pull/3736))
* Add enforce-labels action ([#3816](https://github.com/opensearch-project/sql/pull/3816))
* Add explain ITs with Calcite without pushdown ([#3786](https://github.com/opensearch-project/sql/pull/3786))
* Update the maven snapshot publish endpoint and credential ([#3886](https://github.com/opensearch-project/sql/pull/3886))
* Update commons-lang exclude rule to exclude it everywhere ([#3932](https://github.com/opensearch-project/sql/pull/3932))
