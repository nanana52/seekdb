"""Run paired CREATE VECTOR INDEX and ANN checks on a real seekdb server."""
from __future__ import annotations
import argparse
import json
import math
import re
import statistics
import time
from pathlib import Path
import numpy as np
import pymysql


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=12882)
    parser.add_argument('--user',default='root')
    parser.add_argument('--database',default='amd_hipvs_verify')
    parser.add_argument('--rows',type=int,default=20000)
    parser.add_argument('--trials',type=int,default=3)
    parser.add_argument('--output',type=Path,default=Path('amd_sql_report.json'))
    args=parser.parse_args()
    if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,63}',args.database):
        parser.error('database must be a simple SQL identifier')
    if not 4096<=args.rows<=200000 or not 1<=args.trials<=10:
        parser.error('rows must be 4096..200000 and trials 1..10')
    random=np.random.default_rng(20261116)
    base=random.normal(size=(args.rows,128)).astype('float32')
    queries=random.normal(size=(20,128)).astype('float32')
    ids=np.arange(args.rows,dtype=np.int64)*3+100003
    exact=np.argsort(np.sum(queries**2,axis=1)[:,None]+np.sum(base**2,axis=1)[None,:]-2*queries@base.T,axis=1)[:,:10]
    connection=pymysql.connect(host=args.host,port=args.port,user=args.user,autocommit=True,read_timeout=600,charset='utf8mb4')
    report={'rows':args.rows,'dimensions':128,'M':32,'ef_construction':200,'session_ef_search':512,'trials':[]}
    nonce=str(time.time_ns())
    with connection.cursor() as cursor:
        cursor.execute('CREATE DATABASE IF NOT EXISTS `'+args.database+'`')
        cursor.execute('USE `'+args.database+'`')
        cursor.execute('SET ob_query_timeout=1000000000')
        cursor.execute('SET SESSION ob_hnsw_ef_search=512')
        for trial in range(args.trials):
            timing={}
            for lib in ['vsag','cuvs']:
                table='v_'+nonce+'_'+lib+'_'+str(trial)
                cursor.execute('CREATE TABLE '+table+'(id BIGINT PRIMARY KEY,v VECTOR(128))')
                for start in range(0,args.rows,200):
                    cursor.executemany('INSERT INTO '+table+' VALUES(%s,%s)',[(int(ids[i]),json.dumps(base[i].tolist())) for i in range(start,min(start+200,args.rows))])
                begin=time.monotonic()
                cursor.execute('CREATE VECTOR INDEX idx ON '+table+'(v) WITH(distance=l2,type=hnsw,lib='+lib+',m=32,ef_construction=200,ef_search=512)')
                elapsed=time.monotonic()-begin
                recalls=[]
                for q,query in enumerate(queries):
                    cursor.execute('SELECT id FROM '+table+' ORDER BY l2_distance(v,%s) APPROXIMATE LIMIT 10',(json.dumps(query.tolist()),))
                    got=[row[0] for row in cursor.fetchall()]
                    assert len(got)==10 and len(set(got))==10
                    assert all((value-100003)%3==0 for value in got)
                    recalls.append(len(set(got)&set(int(ids[i]) for i in exact[q]))/10)
                recall=float(np.mean(recalls))
                if recall<0.90:raise RuntimeError('ANN correctness gate failed: '+str((lib,recall)))
                cursor.execute('EXPLAIN SELECT id FROM '+table+' ORDER BY l2_distance(v,%s) APPROXIMATE LIMIT 10',(json.dumps(queries[0].tolist()),))
                plan='\n'.join(str(row) for row in cursor.fetchall())
                if 'VECTOR INDEX' not in plan:raise RuntimeError('Native vector index plan missing')
                timing[lib]={'ddl_sec':elapsed,'recall_at_10':recall,'table':table,'vector_index_plan':True}
                print(json.dumps({'trial':trial,'lib':lib,**timing[lib]}),flush=True)
            report['trials'].append(timing)
    connection.close()
    report['median_ddl_speedup']=statistics.median(t['vsag']['ddl_sec'] for t in report['trials'])/statistics.median(t['cuvs']['ddl_sec'] for t in report['trials'])
    report['status']='passed'
    report['scope']='real SQL DDL timing; verify HIPVS_NATIVE_HNSW_BUILD_COMMITTED in server log to establish GPU use'
    args.output.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report),flush=True)

if __name__=='__main__':
    main()
