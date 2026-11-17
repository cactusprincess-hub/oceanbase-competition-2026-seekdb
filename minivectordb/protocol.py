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

"""The official JSONL process boundary; stdout contains protocol frames only."""
from __future__ import annotations

import json
import os
import sys

from .engine import Engine
from .errors import DatabaseError


def strict_json(text):
    def invalid(value):
        raise ValueError(f'non-finite JSON number: {value}')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result
    return json.loads(text, parse_constant=invalid, object_pairs_hook=pairs)


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def emit(response, rows=None):
    if rows is None:
        sys.stdout.write(encode(response) + '\n')
    else:
        # Result rows and external-sort output are streamed as one physical
        # JSON line: even a large projection need not fit in Python memory.
        sys.stdout.write(encode(response)[:-1] + ',"rows":[')
        separator = ''
        for row in rows:
            sys.stdout.write(separator + encode(row))
            separator = ','
        sys.stdout.write(']}\n')
    sys.stdout.flush()


def main() -> int:
    engine = Engine(os.environ.get('VECTORDB_DATA_DIR', '/data'))
    emit({'type': 'ready'})
    for line in sys.stdin:
        request = {}
        try:
            request = strict_json(line)
            if not isinstance(request, dict) or 'id' not in request:
                raise ValueError('request must be an object with id')
            request_id = request['id']
            if request.get('op') == 'shutdown':
                engine.rollback()
                emit({'id': request_id, 'ok': True})
                return 0
            if request.get('op') != 'execute' or not isinstance(request.get('sql'), str):
                raise ValueError('invalid request')
            result = engine.execute(request['sql'], request.get('search_mode', 'exact'))
            response = {'id': request_id, 'ok': True}
            if result.columns is not None:
                response['columns'] = result.columns
                emit(response, result.rows)
                continue
            if result.affected_rows is not None:
                response['affected_rows'] = result.affected_rows
        except OSError as error:
            # An I/O failure may occur across the atomic publication boundary.
            # Stop rather than acknowledge a commit with uncertain state.
            print(f'fatal storage error: {error}', file=sys.stderr, flush=True)
            return 1
        except DatabaseError as error:
            response = {'id': request.get('id') if isinstance(request, dict) else None,
                        'ok': False, 'error': {'code': error.code, 'message': str(error)}}
        except (ValueError, TypeError, KeyError) as error:
            response = {'id': request.get('id') if isinstance(request, dict) else None,
                        'ok': False, 'error': {'code': 'invalid_request', 'message': str(error)}}
        emit(response)
    engine.rollback()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
