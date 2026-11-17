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

Queries stream one bucket at a time through a bounded 32 MiB page cache. Ordered results use chunked external merge sort under `/data/scratch`; filtered exact TopK uses chunked NumPy arithmetic and a bounded heap when the requested K is small. The `ann` mode currently falls back to exact scanning, preserving Recall@K and filter semantics while trading latency for correctness. The index declaration is persisted, but this version does not yet build a separate ANN structure.

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
