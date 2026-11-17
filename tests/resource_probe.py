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

"""Exercise the submitted database with data larger than its container limit."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'protocol_check'))
from _transport import Session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / 'protocol_check/config.json').read_text())
    config.update(memory='128m', case_timeout=1200, request_timeout=120)
    session = Session(config, args.output/'resource.stderr.log', image=args.image)
    report = {'memory_limit_mib': 128, 'rows': 24000, 'varchar_bytes_per_row': 8192}
    def execute(sql):
        response = session.request(sql)
        assert response['ok'], response
        return response
    try:
        session.start()
        execute('CREATE TABLE big (id BIGINT PRIMARY KEY, content VARCHAR(8192));')
        execute('BEGIN;')
        payload = 'x' * 8192
        for first in range(0, 24000, 100):
            values = ','.join(f"({key},'{payload}')" for key in range(first, first+100))
            assert execute('INSERT INTO big VALUES '+values)['affected_rows'] == 100
        execute('COMMIT;')
        assert execute('SELECT id FROM big ORDER BY id DESC LIMIT 3')['rows'] == [[23999], [23998], [23997]]
        # Unlimited ordering must spill to /data rather than buffer 187.5 MiB
        # of VARCHAR payload inside the 128 MiB container.
        assert execute('SELECT id FROM big ORDER BY id')['rows'] == [[key] for key in range(24000)]
        session.shutdown()
        session.start()
        assert execute('SELECT id FROM big ORDER BY id DESC LIMIT 1')['rows'] == [[23999]]
        execute('BEGIN;')
        execute('DELETE FROM big WHERE id >= 23900;')
        session.crash()
        session.start()
        assert execute('SELECT id FROM big WHERE id >= 23900 ORDER BY id')['rows'] == [[key] for key in range(23900, 24000)]
        session.shutdown()
        report['passed'] = True
        report['payload_mib'] = 24000 * 8192 / (1024 * 1024)
        print(json.dumps(report))
    finally:
        session.close()
    (args.output/'result.json').write_text(json.dumps(report, indent=2)+'\n')


if __name__ == '__main__':
    main()
