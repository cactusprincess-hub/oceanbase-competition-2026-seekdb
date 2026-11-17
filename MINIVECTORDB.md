# MiniVectorDB

This branch implements the single-process JSONL SQL program for the 2026 OceanBase bonus task. The SQL parser is SQLGlot; storage, catalog publication, row encoding, filtering, exact vector search, sorting, and transaction boundaries are implemented here.

## Build and run

```bash
docker build -t minivectordb:local .
mkdir -p /tmp/minivectordb-data
chmod 777 /tmp/minivectordb-data
docker run --rm -i --network=none --read-only \
  --mount type=bind,src=/tmp/minivectordb-data,dst=/data \
  minivectordb:local
```

The process sends `{"type":"ready"}` as its first stdout line, then accepts one JSON request per line. It returns one response per request and flushes immediately. SQL failures use `ok:false`; diagnostics go to stderr. The `shutdown` request rolls back an open transaction and exits cleanly. The program does not listen on a network port.

## Storage and recovery

Signed BIGINT primary keys are distributed over 256 hash buckets. Each bucket is an immutable, compact binary page. Inserts, updates, and deletes write new page files, fsync those files, then atomically replace a checksummed catalog and fsync its directory. The catalog replacement is the commit point. Unreferenced pages are reclaimed after publication; scratch sort runs are removed during recovery. A kill before catalog publication leaves the old catalog authoritative, and a kill after publication leaves the new pages recoverable.

Queries stream one bucket at a time through a bounded 32 MiB page cache. Ordered results use chunked external merge sort under `/data/scratch`; filtered exact TopK uses chunked NumPy arithmetic and a bounded heap when the requested K is small. The `ann` mode currently falls back to exact scanning, preserving Recall@K and filter semantics while trading latency for correctness. A flat index caches float32 matrices by immutable page identity, and persisted declarations are rebuilt before the ready handshake. This version does not build a separate approximate structure; both modes remain exact.

Explicit transactions use copy-on-write catalogs and pages. Reads in a transaction see its staged catalog; `COMMIT` publishes all staged changes in one catalog rename, while `ROLLBACK` drops staged pages. Every autocommit statement uses the same publication boundary. Restart cleanup only removes files not reachable from the committed catalog.

## SQL subset

Supported statements are `CREATE TABLE`, `CREATE VECTOR INDEX`, multi-row `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`, and `ROLLBACK`. Tables require one BIGINT primary key and support BIGINT, VARCHAR(n), and at most one VECTOR(n). SELECT supports projections, `*`, comparison predicates joined by `AND`, `ORDER BY`, `LIMIT`, and `L2_DISTANCE`. Unsupported SQL returns a structured error instead of executing with altered meaning.

## Local verification

The public 20-case check can run without Docker on a development machine:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python protocol_check/run.py \
  --command '["python","-m","minivectordb.protocol"]' \
  --output protocol-results/public
```

This repository includes only the public acceptance package. Public checks do not establish hidden-case admission or competition points. Performance and final scores are determined by the official platform.

## Verified Linux container results

GitHub Actions run [44083737595](https://github.com/cactusprincess-hub/oceanbase-competition-2026-seekdb/actions/runs/44083737595) succeeded for code commit `e6859c9ce1e1c812246ecb851db457671a66ea80`:

- 16 database regression tests.
- Docker image built and all 20 public protocol/SQL cases passed.
- 24,000 rows with 187.5 MiB of VARCHAR payload under a 128 MiB container limit.
- Complete ordered projection, normal restart, and forced termination after an uncommitted delete.

The validation does not include the nine unpublished formal case inputs or the official performance and Agent rankings. The final documentation/evidence commit retains exactly the tested database source, Dockerfile, requirements and tests.

Submission repository: `https://github.com/cactusprincess-hub/oceanbase-competition-2026-seekdb`.
Submission branch: `2026-competition-round2`.
Use a full 40-character SHA from this branch; preserve `agent/conversation.json` in that same commit. Each day permits at most two formal submissions; the environment precheck is separate.
