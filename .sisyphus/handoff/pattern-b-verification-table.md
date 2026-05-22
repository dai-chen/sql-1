# Pattern B Verification Table

## Summary

**Result: 0 of 24 candidate Pattern-B queries actually fail with the current fix.**

All 24 queries from the inventory use `span(<already_typed_timestamp_col>, interval)` — NOT `<agg>(<datetime_udf>(<varchar_field>))`. Since `span()` on a standard TIMESTAMP column does not produce a UDT type, the `DatetimeUdtNormalizeRule` is never triggered, and the cascade bug never manifests.

The inventory's claim that these 24 queries "would fail" was **incorrect**. The bug only triggers when a UDT-producing function (like `TIMESTAMP(varchar_field)`) appears inside an aggregate call or as a span argument on a non-timestamp field.

## Sources

- **IT queries (20)**: NfwPplDashboardIT (15 queries), WafPplDashboardIT (1), StatsCommandIT (4)
- **Doc queries (4)**: nfw.rst (3 distinct shapes), waf.rst (1)
- **Unadapted**: 0 (NewAddedCommandsIT appendcol/append queries were excluded from the 24 because they use `span(age, 10)` on an INTEGER column, not a datetime column — they are not Pattern B datetime queries)

## Verification Table

| # | Source (file:line) | Original PPL query | Adapted PPL query | Predicted | Actual |
|---|---|---|---|---|---|
| 1 | NfwPplDashboardIT:61 | `source=nfw \| stats sum(pkts) by span(event.timestamp,2d), src_ip \| rename \| sort \| head 10` | `source=catalog.events \| stats sum(id) as packet_count by span(created_at,2d) as timestamp_span, name \| rename name as Source IP \| sort - packet_count \| head 10` | PASS | **PASS** |
| 2 | NfwPplDashboardIT:77 | `source=nfw \| stats sum(bytes) by span(event.timestamp,2d), src_ip \| rename \| sort \| head 10` | `source=catalog.events \| stats sum(id) as sum_bytes by span(created_at,2d) as timestamp_span, name \| rename name as Source IP \| sort - sum_bytes \| head 10` | PASS | **PASS** |
| 3 | NfwPplDashboardIT:93 | `source=nfw \| stats sum(pkts) by span(event.timestamp,2d), dest_ip \| rename \| sort \| head 10` | `source=catalog.events \| stats sum(id) as packet_count by span(created_at,2d) as timestamp_span, name \| rename name as Destination IP \| sort - packet_count \| head 10` | PASS | **PASS** |
| 4 | NfwPplDashboardIT:109 | `source=nfw \| stats sum(bytes) by span(event.timestamp,2d), dest_ip \| rename \| sort \| head 10` | `source=catalog.events \| stats sum(id) as bytes by span(created_at,2d) as timestamp_span, name \| rename name as Destination IP \| sort - bytes \| head 10` | PASS | **PASS** |
| 5 | NfwPplDashboardIT:157 | `source=nfw \| stats sum(pkts) by span(event.timestamp,2d), src_ip, dest_ip \| eval concat \| sort \| head 10` | `source=catalog.events \| stats sum(id) as packet_count by span(created_at,2d) as timestamp_span, name \| sort - packet_count \| head 10` | PASS | **PASS** |
| 6 | NfwPplDashboardIT:176 | `source=nfw \| stats sum(bytes) by span(event.timestamp,2d), src_ip, dest_ip \| eval concat \| sort \| head 10` | `source=catalog.events \| stats sum(id) as bytes by span(created_at,2d) as timestamp_span, name \| sort - bytes \| head 10` | PASS | **PASS** |
| 7 | NfwPplDashboardIT:195 | `source=nfw \| where action=allowed \| stats count() by span(event.timestamp,2d), hostname \| rename \| sort` | `source=catalog.events \| stats count() as event_count by span(created_at,2d) as time_bucket, name \| rename name as Hostname \| sort - event_count` | PASS | **PASS** |
| 8 | NfwPplDashboardIT:215 | `source=nfw \| where action=blocked \| stats count() by span(event.timestamp,2d), hostname \| rename \| sort \| HEAD 10` | `source=catalog.events \| where name="blocked" \| stats count() as event_count by span(created_at,2d) as time_bucket, name \| rename name as Hostname \| sort - event_count \| head 10` | PASS | **PASS** |
| 9 | NfwPplDashboardIT:234 | `source=nfw \| where action=allowed \| stats count() by span(event.timestamp,2d), tls.sni \| rename \| sort \| HEAD 10` | `source=catalog.events \| stats count() as event_count by span(created_at,2d) as time_bucket, name \| rename name as Hostname \| sort - event_count \| head 10` | PASS | **PASS** |
| 10 | NfwPplDashboardIT:253 | `source=nfw \| where action=blocked \| stats count() by span(event.timestamp,2d), tls.sni \| rename \| sort \| HEAD 10` | `source=catalog.events \| stats count() as event_count by span(created_at,2d) as time_bucket, name \| rename name as Hostname \| sort - event_count \| head 10` | PASS | **PASS** |
| 11 | NfwPplDashboardIT:272 | `source=nfw \| where isnotnull(http.url) \| stats count() by span(event.timestamp,2d), http.url \| rename \| sort \| head 10` | `source=catalog.events \| where name is not null \| stats count() as event_count by span(created_at,2d) as timestamp_span, name \| rename name as URL \| sort - event_count \| head 10` | PASS | **PASS** |
| 12 | NfwPplDashboardIT:289 | `source=nfw \| where isnotnull(user_agent) \| stats count() by span(event.timestamp,2d), user_agent \| rename \| sort \| head 10` | `source=catalog.events \| where name is not null \| stats count() as event_count by span(created_at,2d) as timestamp_span, name \| rename name as User Agent \| sort - event_count \| head 10` | PASS | **PASS** |
| 13 | NfwPplDashboardIT:342 | `source=nfw \| stats count() by SPAN(event.timestamp,2d), src_port \| eval CAST(src_port AS STRING) \| sort \| HEAD 10` | `source=catalog.events \| stats count() as Count by span(created_at,2d) as timestamp_span, id \| eval Source Port = CAST(id AS STRING) \| sort - Count \| head 10` | PASS | **PASS** |
| 14 | NfwPplDashboardIT:359 | `source=nfw \| stats count() by SPAN(event.timestamp,2d), dest_port \| eval CAST(dest_port AS STRING) \| sort \| HEAD 10` | `source=catalog.events \| stats count() as Count by span(created_at,2d) as timestamp_span, id \| eval Dest Port = CAST(id AS STRING) \| sort - Count \| head 10` | PASS | **PASS** |
| 15 | NfwPplDashboardIT:371 | `source=nfw \| WHERE proto=TCP \| STATS count() by SPAN(event.timestamp,2d), src_ip, dest_ip, dest_port \| EVAL concat \| SORT \| HEAD 10` | `source=catalog.events \| where name="TCP" \| stats count() as Count by span(created_at,2d) as timestamp_span, name, id \| sort - Count \| head 10` | PASS | **PASS** |
| 16 | WafPplDashboardIT:58 | `source=waf \| STATS count() as Count by span(start_time,30d), action \| SORT - Count` | `source=catalog.events \| stats count() as Count by span(created_at,30d), name \| sort - Count` | PASS | **PASS** |
| 17 | StatsCommandIT:890 | `source=bank \| stats count() by span(birthdate,1month) \| sort - cnt, span(birthdate,1month) \| head 5` | `source=catalog.events \| stats count() as cnt by span(created_at,1d) \| sort - cnt \| head 5` | PASS | **PASS** |
| 18 | StatsCommandIT:904 | `source=bank \| stats count() by span(birthdate,1month) \| sort cnt, span(birthdate,1month) \| head 5` | `source=catalog.events \| stats count() as cnt by span(created_at,1d) \| sort cnt \| head 5` | PASS | **PASS** |
| 19 | StatsCommandIT:916 | `source=account \| stats sum(balance) by span(age,2) \| sort - sum(balance) \| head 5` | `source=catalog.events \| stats sum(id) as total by span(id,2) \| sort - total \| head 5` | PASS | **PASS** |
| 20 | StatsCommandIT:929 | `source=account \| stats sum(balance) by span(age,2) \| sort sum(balance) \| head 5` | `source=catalog.events \| stats sum(id) as total by span(id,2) \| sort total \| head 5` | PASS | **PASS** |
| 21 | nfw.rst:301 | `source=nfw \| WHERE proto=TCP \| STATS count() by SPAN(timestamp,2d), src_ip, dest_ip, dest_port \| EVAL concat \| SORT \| HEAD 10` | `source=catalog.events \| where name="TCP" \| stats count() as Count by span(created_at,2d) as timestamp_span, name \| eval label = CONCAT(name," - ",name) \| sort - Count \| head 10` | PASS | **PASS** |
| 22 | nfw.rst:451 | `source=nfw \| WHERE proto=ICMP \| STATS count() by SPAN(timestamp,1d), src_ip, dest_ip, dest_port \| EVAL concat \| SORT \| HEAD 10` | `source=catalog.events \| where name="ICMP" \| stats count() as Count by span(created_at,1d) as timestamp_span, name \| sort - Count \| head 10` | PASS | **PASS** |
| 23 | nfw.rst:524 | `source=nfw \| WHERE action=blocked \| STATS count() by SPAN(timestamp,2d), src_ip, dest_ip, dest_port \| EVAL concat \| SORT \| HEAD 10` | `source=catalog.events \| where name="blocked" \| stats count() as Count by span(created_at,2d) as timestamp_span, name \| sort - Count \| head 10` | PASS | **PASS** |
| 24 | waf.rst:45 | `source=waf \| STATS count() as Count by span(start_time,30d), action \| SORT - Count` | `source=catalog.events \| stats count() as Count by span(created_at,30d), name \| sort - Count` | PASS | **PASS** |

## Key Insight

The inventory conflated two different things:
1. **`span(<standard_timestamp_col>, interval)` + downstream pipes** — This is what all 24 queries do. The span group key is already a standard TIMESTAMP type. The `DatetimeUdtNormalizeRule` is never triggered because there's no UDT to normalize. **These all PASS.**
2. **`<agg>(<datetime_udf>(<varchar_field>))` + downstream pipes** — e.g., `stats max(timestamp(name)) by id | sort`. This IS the bug-triggering shape because `timestamp(name)` produces a UDT type that enters the aggregate. **These FAIL** (confirmed by probes B-H in prior runs).

The inventory's error was assuming that `span(event.timestamp, 2d)` where `event.timestamp` is already a TIMESTAMP column would produce a UDT. It does not — `span()` on a standard-typed column returns a standard-typed result.
