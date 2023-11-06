# Problem statement
When you read incrementally from the moment before the first commit, the behavior is
different, depending on whether clean happened or not. If you read from commit time `0`
and old commits were never deleted, you will receive all the updates(dataset equal to snapshot).
But if you are writing for a long time, so some old commits were deleted, you will receive
only updates from all commits except the first.

## Steps
Correct case verification:
1. Create a hudi table with a sufficient number of commits retained
   (e.g. in this case using the defaults) and ingest 5 times(5 commits are retained),
   hudi creates 5 distinct commit times.
2. Try to read incrementally with very start(e.g. from `0` commit time).

Expected: The incremental result row count is equal to the snapshot row count.
Actual: The incremental result row count is equal to the snapshot row count.

Incorrect case verification:
1. Create a hudi table with low number of commits retained
   and ingest 5 times(less than 5 commits retained in the result).
2. Try to read incrementally with very start(e.g. from `0` commit time).

Expected: The incremental result row count is equal to the snapshot row count.
Actual: The incremental result row count less than snapshot row count and includes updates
from all commits except the first(which becomes kind of a base file).

## Hudi versions
I checked that this behavior happens for `0.9.0` and `0.10.0`, but works correctly starting from `0.11.0`.

## Local run
Just call `sbt run` and filter logs by `com.example.hudi.HudiIncrementalChecker`.

## Fixes
I assume [this](https://github.com/apache/hudi/pull/3946/files) PR might introduce fix in `0.11.0`.