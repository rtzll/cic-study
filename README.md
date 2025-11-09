# Concurrent Index Creation Study

Test `CREATE INDEX CONCURRENTLY` (`CIC`) scenarios to observe how a follow-up
`CREATE INDEX IF NOT EXISTS` behaves at different catalog states (no entry, not
ready, ready-but-invalid, validation phase, steady state). It spins up
disposable Postgres containers, seeds data, runs CIC, then issues the
non-concurrent variant while writers/probes keep pressure on the table. Logs are
timestamped relative to each scenario to compare durations linearly.

## TL;DR

- Once a concurrent build fully commits (`sequential`), the follow-up
  `CREATE INDEX IF NOT EXISTS` is a clean no-op.
- If the non-concurrent command starts before a catalog entry exists
  (`before-catalog`), it builds the index outright while holding the usual
  `RowExclusiveLock`; CIC wakes up, sees a ready+valid index, and exits
  immediately.
- As soon as the catalog row exists but CIC is still building or validating
  (`exists-not-ready`, `ready-not-valid`, `validation-phase`), overlapping
  creation almost always trips Postgres’ deadlock detector. In **most** of runs
  the database cancels the concurrent build and lets the non-concurrent build
  finish; on rare runs the cancellation flips the other way.
- Very occasionally the `ready-not-valid` or `validation-phase` overlaps
  complete without error, but the common outcome remains a deadlock cancellation
  once both builders contend for the same locks.

## Scenarios

| `key`              | Explanation                                                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sequential`       | Baseline run: finish CIC so the index is ready+valid, then immediately issue `CREATE INDEX IF NOT EXISTS` to confirm it no-ops.                     |
| `before-catalog`   | Overlap where the non-concurrent command starts before CIC creates a catalog entry, so `idx=<missing>` shows up until the plain build finishes.     |
| `exists-not-ready` | Collides with CIC while it is still scanning/sorting; catalog row exists but `indisready=false`, so IF NOT EXISTS can block on share-update locks.  |
| `ready-not-valid`  | Triggers once CIC finished building (`indisready=true`) but before validation flips `indisvalid`, capturing the gap between build and validation.   |
| `validation-phase` | Fires during CIC’s validation pipeline (scanning index/table, waiting for snapshots) to show how IF NOT EXISTS behaves during that critical window. |

Run `go run . [scenario-key]` to focus on a subset; omit args to execute all.

## Observed CIC Phases

### Non-concurrent Starts First (`before-catalog`)

1. `idx=<missing> progress=building index: scanning table`

   The concurrent worker starts, but still reports the index as missing while it
   parses and scans the table.

2. `idx=<missing> progress=building index: sorting live tuples`

   The concurrent worker advances to sorting while the non-concurrent build
   continues to hold the lead.

3. `idx ready=true valid=true progress=<inactive>`

   The concurrent session wakes up, notices the index is already valid, and
   exits without entering the validation phase.

### CIC Starts First

_Note_: not each progress change shows up on every run (despite 1ms polling).

1. `idx=<missing> progress=building index: scanning table`

   No catalog row yet; CIC is still parsing/scanning.

2. `idx ready=false valid=false progress=building index: scanning table`

   A catalog entry exists but the scan is ongoing.

3. `idx ready=false valid=false progress=building index: sorting live tuples`

   Scan completed; tuples are being sorted.

4. `idx ready=false valid=false progress=building index: loading tuples in tree`

   Sorted tuples are inserted into the index structure.

5. `idx ready=true valid=false progress=waiting for writers before validation`

   Build phase is done; waiting to start validation while writers drain.

6. `idx ready=true valid=false progress=index validation: scanning index`

   Validation begins by scanning the new index.

7. `idx ready=true valid=false progress=index validation: sorting tuples`

   Validation sorts tuples for comparison.

8. `idx ready=true valid=false progress=index validation: scanning table`

   Validation cross-checks rows against the table.

9. `idx ready=true valid=false progress=waiting for old snapshots`

   Validation is done; waiting for old snapshots before commit.

10. `idx ready=true valid=false progress=<inactive>`

   The CIC session finished but `indisvalid` hasn’t flipped yet.

## Example Output of `validation-phase` Run

```
[+0.0000s] ==== Scenario 5: state: validation phase before commit ====
[+0.0000s] [validation-phase] starting Postgres 16.8 container
[+1.5133s] [validation-phase] Postgres up at postgres://u:pw@localhost:33201/db?sslmode=disable
[+1.5221s] [validation-phase] creating table t
[+2.6346s] [validation-phase] seed of 1000000 rows completed in 1110.36 ms
[+2.6346s] [validation-phase] launching CREATE INDEX CONCURRENTLY in background
[+2.6346s] [validation-phase] waiting for catalog entry that is ready and valid (but CIC not committed)
[+2.6448s] [probe] started
[+2.6456s] [probe] insert #01 completed after 0.75 ms
[+2.6461s] [cic] started
[+2.6485s] [cic] snapshot updated: idx=<missing> progress=building index: scanning table
[+2.6492s] [writers] started
[+2.6493s] [cic] snapshot updated: idx ready=false valid=false progress=building index: scanning table
[+2.6874s] [cic] snapshot updated: idx ready=false valid=false progress=building index: sorting live tuples
[+2.7455s] [probe] insert #02 completed after 0.56 ms
[+2.8452s] [probe] insert #03 completed after 0.28 ms
[+2.9454s] [probe] insert #04 completed after 0.45 ms
[+3.0453s] [probe] insert #05 completed after 0.36 ms
[+3.1453s] [probe] insert #06 completed after 0.43 ms
[+3.2452s] [probe] insert #07 completed after 0.31 ms
[+3.3454s] [probe] insert #08 completed after 0.49 ms
[+3.4453s] [probe] insert #09 completed after 0.41 ms
[+3.5453s] [probe] insert #10 completed after 0.38 ms
[+3.6517s] [probe] insert #11 completed after 1.15 ms
[+3.7458s] [probe] insert #12 completed after 0.87 ms
[+3.7587s] [cic] snapshot updated: idx ready=false valid=false progress=building index: loading tuples in tree
[+3.8556s] [probe] insert #13 completed after 10.71 ms
[+3.9468s] [probe] insert #14 completed after 1.85 ms
[+3.9573s] [cic] snapshot updated: idx ready=true valid=false progress=waiting for writers before validation
[+3.9573s] [validation-phase] IF NOT EXISTS trigger: idx ready=true valid=false
[+3.9573s] [validation-phase] starting non-concurrent CREATE INDEX IF NOT EXISTS idx
[+3.9596s] [validation-phase] locks before non-concurrent build: RowExclusiveLock, ShareUpdateExclusiveLock
[+3.9845s] [cic] snapshot updated: idx ready=true valid=false progress=index validation: scanning index
[+4.0084s] [cic] snapshot updated: idx ready=true valid=false progress=index validation: sorting tuples
[+4.0483s] [cic] snapshot updated: idx ready=true valid=false progress=index validation: scanning table
[+4.0765s] [cic] snapshot updated: idx ready=true valid=false progress=waiting for old snapshots
[+5.0529s] [probe] insert #15 completed after 1007.97 ms
[+6.0587s] [cic] failed after 3411.82 ms: ERROR: deadlock detected (SQLSTATE 40P01)
[+6.0641s] [probe] insert #16 completed after 1011.18 ms
[+6.0645s] [validation-phase] non-concurrent build finished in 2104.78 ms
[+6.0672s] [probe] insert #17 completed after 3.03 ms
[+6.0675s] [validation-phase] locks after non-concurrent build: RowExclusiveLock
[+6.0682s] [cic] snapshot updated: idx ready=true valid=false progress=<inactive>
[+6.0682s] [probe] stopping
[+6.0682s] [writers] stopping
[+6.3459s] [validation-phase] finished with error: ERROR: deadlock detected (SQLSTATE 40P01)
```
