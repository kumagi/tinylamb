#!/usr/bin/env python3
"""TPC-C W=1: identical existing C++ generator and TCP client for Tiny/PG."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import shutil
import socket
import subprocess
from types import SimpleNamespace

from run import Client, Stack, append_json
from workloads import ROOT, adapt_sql, equivalent

TABLES = ['warehouse', 'district', 'customer', 'history', 'item', 'stock', 'orders', 'new_order', 'order_line']
KEYS = [('warehouse', 'w_id', True), ('district', 'd_w_id,d_id', True),
        ('customer', 'c_w_id,c_d_id,c_id', True), ('customer', 'c_w_id,c_d_id,c_last,c_first', False),
        ('item', 'i_id', True), ('stock', 's_w_id,s_i_id', True),
        ('orders', 'o_w_id,o_d_id,o_id', True), ('orders', 'o_w_id,o_d_id,o_c_id,o_id', False),
        ('new_order', 'no_w_id,no_d_id,no_o_id', True),
        ('order_line', 'ol_w_id,ol_d_id,ol_o_id,ol_number', True)]


def clone(prefix, target):
    for f in prefix.parent.glob(prefix.name + '.*'):
        if f.is_file():
            shutil.copy2(f, target.parent / (target.name + f.name[len(prefix.name):]))


def check(client):
    counts = {t: client.execute(f'SELECT COUNT(*) FROM {t}')[0][0] for t in TABLES}
    dnext = client.execute('SELECT SUM(d_next_o_id), SUM(d_ytd) FROM district')[0]
    warehouse = client.execute('SELECT SUM(w_ytd) FROM warehouse')[0][0]
    history = client.execute('SELECT SUM(h_amount) FROM history')[0][0]
    lines = client.execute('SELECT SUM(o_ol_cnt) FROM orders')[0][0]
    valid = (counts['warehouse'] == 1 and counts['district'] == 10 and counts['customer'] == 30000
             and counts['item'] == counts['stock'] == 100000
             and dnext[0] == counts['orders'] + 10 and lines == counts['order_line']
             and equivalent([(warehouse,)], [(dnext[1],)])
             and equivalent([(warehouse,)], [(history,)]))
    return dict(valid=valid, counts=counts, district=list(dnext), warehouse=warehouse, history=history, order_lines=lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--server', type=Path, required=True)
    p.add_argument('--client', type=Path, required=True)
    p.add_argument('--fixture', type=Path, required=True)
    p.add_argument('--seconds', type=int, default=30)
    p.add_argument('--warmup', type=int, default=5)
    p.add_argument('--trials', type=int, default=3)
    p.add_argument('--measure-gate',type=Path)
    a = p.parse_args()
    out = a.output.resolve(); out.mkdir(parents=True, exist_ok=False)
    sockets = [socket.socket() for _ in range(3)]
    for s in sockets: s.bind(('127.0.0.1', 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets: s.close()
    args = SimpleNamespace(server=str(a.server.resolve()), scratch=str(ROOT/'build'), server_cpus='0-15',
        tiny_port=ports[0], pg_port=ports[1], mysql_port=ports[2], read_workers=8,
        tiny_query_memory_mib=256, server_metrics=True,
        postgres_image='postgres@sha256:979bccfde35445db618c65107d27d08c6c21339d8dc3ad4c830c6633947be0e4',
        mysql_image='mysql@sha256:a0225d1ea6631c8bc2385dc4c2faba17bbf1834ecf369e1659a9a382bd8d5dd7')
    stack = Stack(args, out)
    clone(a.fixture, stack.work/'database')
    try:
        stack.start()
        meta = stack.metadata(); meta['client_sha256'] = hashlib.file_digest(a.client.open('rb'), 'sha256').hexdigest()
        meta['fixture'] = str(a.fixture)
        metadata=a.fixture.parent/(a.fixture.name+'.log.meta')
        meta['fixture_wal_metadata_hex']=metadata.read_bytes().hex()
        (out/'environment.json').write_text(json.dumps(meta, indent=2))
        tiny = Client('tinylamb', stack.ports, 60)
        initial = check(tiny); assert initial['valid'], initial
        (out/'initial-invariants.json').write_text(json.dumps(initial, indent=2, default=str))
        for table in TABLES:
            rows = tiny.execute(f'SELECT * FROM {table}')
            with (out/f'fixture-{table}.csv').open('w') as f:
                # Quote all strings, including the empty string; only actual
                # NULL is an unquoted empty CSV field.
                for row in rows:
                    f.write(','.join('' if x is None else ('"'+x.replace('"','""')+'"' if isinstance(x,str) else str(x)) for x in row)+'\n')
        tiny.close()
        source = (ROOT/'benchmark/tpcc_workload.cpp').read_text()
        text = source.split('constexpr std::array<std::string_view, 9> schema = {',1)[1].split('};',1)[0]
        ddls = re.findall(r'((?:"[^"\n]*"\s*)+)(?:,|$)',text,re.M)
        schema = [''.join(re.findall(r'"([^"\n]*)"',s)) for s in ddls]
        assert len(schema)==9, schema
        pg = Client('postgresql',stack.ports,120)
        for ddl in schema:
            ddl = re.sub(r'\bINT64\b','BIGINT',ddl)
            ddl = re.sub(r'\bNUMERIC\b','DOUBLE PRECISION',ddl)
            ddl = re.sub(r'\bSTRING\b','TEXT',ddl)
            pg.execute(ddl)
        for i,(table,key,unique) in enumerate(KEYS):
            pg.execute(f'CREATE {"UNIQUE " if unique else ""}INDEX tpcc_{i} ON {table} ({key})')
        pg.close()
        for trial in range(1,a.trials+1):
            if trial>1:
                stack.process.terminate(); stack.process.wait(timeout=30)
                db = stack.work/f'database-{trial}'; clone(a.fixture,db)
                env={k:v for k,v in os.environ.items() if not k.startswith('TINYLAMB_')}
                env.update(TINYLAMB_PAGE_POOL_BYTES='536870912',TINYLAMB_QUERY_MEMORY_BYTES=str(256<<20),TINYLAMB_SERVER_METRICS='1')
                argv=['taskset','-c','0-15',str(a.server.resolve()),str(db),'--port',str(ports[0]),'--read-workers','8']
                append_json(out/'commands.jsonl',dict(argv=argv,env={k:v for k,v in env.items() if k.startswith('TINYLAMB_')}))
                stack.process=subprocess.Popen(argv,env=env,stdout=stack.log,stderr=subprocess.STDOUT,cwd=ROOT)
                import time
                for _ in range(600):
                    try:
                        c=Client('tinylamb',stack.ports);c.close();break
                    except Exception:time.sleep(.1)
                else:raise RuntimeError('Tiny restart timeout')
            pg=Client('postgresql',stack.ports,120)
            pg.execute('TRUNCATE '+','.join(TABLES))
            for table in TABLES:
                with (out/f'fixture-{table}.csv').open() as f:
                    pg.cursor.copy_expert(f'COPY {table} FROM STDIN WITH (FORMAT csv)',f)
                pg.execute('VACUUM (ANALYZE) '+table)
            state=check(pg);assert state['valid'] and state['counts']==initial['counts'],state
            pg.close()
            if trial==1:
                (out/'prepared.json').write_text(json.dumps(dict(prepared=True)))
                if a.measure_gate:
                    import time
                    print('prepared; waiting for measure gate',a.measure_gate,flush=True)
                    while not a.measure_gate.exists():time.sleep(.2)
            engines=['tinylamb','postgresql'];random.Random(20261030+trial).shuffle(engines)
            for engine in engines:
                argv=[str(a.client.resolve()),'--port',str(stack.ports[engine]),'--clients','10',
                      '--seconds',str(a.seconds),'--warmup',str(a.warmup),'--seed','20260819']
                append_json(out/'commands.jsonl',dict(argv=argv,engine=engine,trial=trial))
                print('START',engine,trial,flush=True)
                with (out/f'{engine}-{trial}.log').open('w') as f:
                    result=subprocess.run(argv,stdout=f,stderr=subprocess.STDOUT,timeout=a.seconds+a.warmup+120)
                text=(out/f'{engine}-{trial}.log').read_text()
                metrics=dict(re.findall(r'^([a-z_0-9.]+)=(.*)$',text,re.M))
                c=Client(engine,stack.ports,120);state=check(c);c.close()
                record=dict(kind='tpcc',engine=engine,trial=trial,seconds=a.seconds,warmup=a.warmup,
                            metrics=metrics,invariants=state,exit_code=result.returncode,
                            valid=result.returncode==0 and state['valid'])
                append_json(out/'results.jsonl',record)
                print('DONE',engine,trial,'tps',metrics.get('tps'),'valid',record['valid'],flush=True)
                assert record['valid'],record
        (out/'finished.json').write_text(json.dumps(dict(trials=a.trials,engines=2)))
    finally:
        stack.close()


if __name__=='__main__':
    main()
