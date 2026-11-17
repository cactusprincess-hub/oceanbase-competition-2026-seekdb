# Copyright (c) 2021 OceanBase
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import copy
import json
import os
import random
import signal
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from minivectordb.engine import Engine
from minivectordb.errors import DatabaseError
from minivectordb.sorting import external_sort


def rows(db, sql):
    return list(db.execute(sql).rows)


def schema(db):
    db.execute('CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(8), embedding VECTOR(2));')


@pytest.mark.parametrize('bad', ["'[0]'", "'[0,1,2]'", "'[NaN,0]'", "'[Infinity,0]'", "'[1e100,0]'", "'[true,0]'", "'[[0],0]'", "'not-json'"])
def test_failed_statement_preserves_transaction(tmp_path, bad):
    db = Engine(tmp_path)
    schema(db)
    db.execute("INSERT INTO docs VALUES (1,'old','[0,0]');")
    db.execute('BEGIN;')
    db.execute("UPDATE docs SET category='keep' WHERE id=1;")
    with pytest.raises(DatabaseError):
        db.execute(f"INSERT INTO docs VALUES (2,'new','[1,1]'),(3,'bad',{bad});")
    assert rows(db, 'SELECT id,category FROM docs ORDER BY id;') == [[1, 'keep']]
    db.commit()
    assert rows(Engine(tmp_path), 'SELECT id,category FROM docs ORDER BY id;') == [[1, 'keep']]


def test_bigint_unicode_descending_and_query_validation(tmp_path):
    db = Engine(tmp_path)
    db.execute('CREATE TABLE docs (id BIGINT PRIMARY KEY, value VARCHAR(3));')
    db.execute("INSERT INTO docs VALUES (-9223372036854775808,'猫🐈a'),(9223372036854775807,'猫🐈a'),(0,'乙');")
    assert rows(db, 'SELECT id FROM docs ORDER BY value DESC;') == [[-9223372036854775808], [9223372036854775807], [0]]
    for sql in ["INSERT INTO docs VALUES (3,'abcd');", "INSERT INTO docs VALUES (9223372036854775808,'x');", "UPDATE docs SET value='abcd' WHERE id=4;"]:
        with pytest.raises(DatabaseError):
            db.execute(sql)
    for sql in ['SELECT missing FROM docs;', 'SELECT id FROM docs WHERE id=1 OR id=2;', 'SELECT DISTINCT value FROM docs;',
                "UPDATE docs SET value='bad' LIMIT 1;", 'DELETE FROM docs LIMIT 1;', 'SELECT a.id FROM docs a JOIN docs b ON a.id=b.id;']:
        with pytest.raises(DatabaseError):
            db.execute(sql)
    assert len(rows(db, 'SELECT id FROM docs;')) == 3


def test_empty_table_rejects_invalid_inputs(tmp_path):
    db = Engine(tmp_path)
    schema(db)
    for sql in ["UPDATE docs SET embedding='[1]' WHERE id=9;", 'SELECT id FROM docs WHERE id=1 OR id=2;',
                "SELECT L2_DISTANCE(embedding,'[1]') AS d FROM docs;", 'SELECT missing FROM docs;']:
        with pytest.raises(DatabaseError):
            db.execute(sql)


def test_multitable_and_schema_rollback(tmp_path):
    db = Engine(tmp_path)
    schema(db)
    db.execute('BEGIN;')
    db.execute('CREATE TABLE second (id BIGINT PRIMARY KEY, n BIGINT);')
    db.execute('INSERT INTO second VALUES (8,9);')
    db.execute("INSERT INTO docs VALUES (1,'one','[2,3]');")
    db.rollback()
    assert 'second' not in db.tables
    assert rows(db, 'SELECT id FROM docs;') == []
    db.execute('BEGIN;')
    db.execute('CREATE TABLE second (id BIGINT PRIMARY KEY, n BIGINT);')
    db.execute('INSERT INTO second VALUES (8,9);')
    db.execute("INSERT INTO docs VALUES (1,'one','[2,3]');")
    db.commit()
    restored = Engine(tmp_path)
    assert rows(restored, 'SELECT id,n FROM second;') == [[8, 9]]
    assert rows(restored, 'SELECT id FROM docs;') == [[1]]


def test_randomized_transaction_visibility_and_exact_recall(tmp_path):
    rng = random.Random(4181)
    db = Engine(tmp_path)
    schema(db)
    model = {}
    committed = {}
    in_transaction = False
    for step in range(120):
        if step % 12 == 0:
            if in_transaction:
                if step % 24:
                    db.commit()
                    committed = copy.deepcopy(model)
                else:
                    db.rollback()
                    model = copy.deepcopy(committed)
            db.execute('BEGIN;')
            in_transaction = True
        key = rng.randrange(-15, 35)
        vector = np.asarray([rng.uniform(-3, 3), rng.uniform(-3, 3)], dtype=np.float32).tolist()
        category = str(rng.randrange(3))
        if key not in model:
            db.execute(f"INSERT INTO docs VALUES ({key},'{category}','{json.dumps(vector)}');")
            model[key] = (category, vector)
        elif rng.randrange(2):
            db.execute(f"UPDATE docs SET category='{category}',embedding='{json.dumps(vector)}' WHERE id={key};")
            model[key] = (category, vector)
        else:
            db.execute(f'DELETE FROM docs WHERE id={key};')
            del model[key]
        assert rows(db, 'SELECT id FROM docs ORDER BY id;') == [[key] for key in sorted(model)]
        query = np.asarray([.3, -.7], dtype=np.float32).astype(np.float64)
        expected = sorted((float(np.linalg.norm(np.asarray(v, dtype=np.float64)-query)), key)
                          for key, (c, v) in model.items() if c == '1')[:5]
        actual = rows(db, "SELECT id,L2_DISTANCE(embedding,'[0.3,-0.7]') AS d FROM docs WHERE category='1' ORDER BY d LIMIT 5;")
        assert [row[0] for row in actual] == [key for _, key in expected]
        assert np.allclose([row[1] for row in actual], [d for d, _ in expected])
    db.rollback()
    assert rows(db, 'SELECT id FROM docs ORDER BY id;') == [[key] for key in sorted(committed)]


def test_external_sort_spills_and_cleans(tmp_path):
    output = external_sort(((i % 31, i, 'x'*128) for i in reversed(range(5000))), lambda x: x[:2], tmp_path, budget=8192)
    result = list(output)
    assert result == sorted(result, key=lambda x: x[:2])
    assert len(result) == 5000
    assert not list(tmp_path.iterdir())


def child(code, root):
    return subprocess.run([sys.executable, '-c', code, str(root)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)


@pytest.mark.parametrize('when', ['before', 'after'])
def test_crash_at_catalog_publication_is_atomic(tmp_path, when):
    db = Engine(tmp_path)
    db.execute('CREATE TABLE a (id BIGINT PRIMARY KEY, n BIGINT);')
    db.execute('CREATE TABLE b (id BIGINT PRIMARY KEY, n BIGINT);')
    db.execute('INSERT INTO a VALUES (1,0);')
    db.execute('INSERT INTO b VALUES (1,0);')
    code = '''
import os, signal, sys
from minivectordb.engine import Engine
import minivectordb.storage as storage
db=Engine(sys.argv[1]); db.execute('BEGIN;')
db.execute('UPDATE a SET n=1;'); db.execute('UPDATE b SET n=1;')
original=storage.os.replace
def replace(src,dst):
    WHEN_BEFORE
    original(src,dst)
    WHEN_AFTER
storage.os.replace=replace
db.commit()
'''.replace('WHEN_BEFORE', 'os.kill(os.getpid(),signal.SIGKILL)' if when == 'before' else 'pass').replace('WHEN_AFTER', 'os.kill(os.getpid(),signal.SIGKILL)' if when == 'after' else 'pass')
    result = child(code, tmp_path)
    assert result.returncode == -signal.SIGKILL, result.stderr
    restored = Engine(tmp_path)
    expected = [[0]] if when == 'before' else [[1]]
    assert rows(restored, 'SELECT n FROM a;') == expected
    assert rows(restored, 'SELECT n FROM b;') == expected


def test_uncommitted_crash_and_repeat_recovery(tmp_path):
    db = Engine(tmp_path)
    schema(db)
    db.execute("INSERT INTO docs VALUES (1,'saved','[1,2]');")
    result = child('''
import os,signal,sys
from minivectordb.engine import Engine
db=Engine(sys.argv[1]); db.execute('BEGIN;')
db.execute("UPDATE docs SET category='lost',embedding='[0,0]';")
os.kill(os.getpid(),signal.SIGKILL)
''', tmp_path)
    assert result.returncode == -signal.SIGKILL
    result = child('''
import os,signal,sys
from pathlib import Path
from minivectordb.engine import Engine
old=Path.unlink
def unlink(path,*args,**kwargs):
    old(path,*args,**kwargs)
    os.kill(os.getpid(),signal.SIGKILL)
Path.unlink=unlink
Engine(sys.argv[1])
''', tmp_path)
    assert result.returncode == -signal.SIGKILL
    for _ in range(3):
        assert rows(Engine(tmp_path), 'SELECT id,category FROM docs;') == [[1, 'saved']]
