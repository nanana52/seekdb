from pathlib import Path
import time
p=Path('/opt/oceanbase_completion_20261116/fts_count_cardinality')
for _ in range(2000):
 text=(p/'semantics.log').read_text(errors='replace')
 if 'COUNT_CARDINALITY_FULL_SEMANTICS_PASS' in text:break
 if 'Traceback' in text:raise RuntimeError(text[-1600:])
 time.sleep(1)
else:raise TimeoutError('Cardinality semantics pending')
from pathlib import Path
import os,subprocess,time,json,re,statistics,pymysql
b=Path('/opt/oceanbase_completion_20261116');root=b/'seekdb';logs=b/'fts_count_cardinality';env=dict(os.environ,DOCKER_HOST='unix://'+str(b/'docker/docker.sock'));data=logs/'paired_container_data_count_final';data.mkdir(exist_ok=False)
for _ in range(1200):
 s=(b/'fts_parallel_build/actual_container_guards.log').read_text(errors='replace')
 if 'ALL_REAL_CGROUP_GUARDS_AND_SHARED_DDL_PASS' in s:break
 if 'Traceback' in s:raise RuntimeError(s[-2000:])
 time.sleep(1)
else:raise TimeoutError('Real cgroup profile tests pending')
for _ in range(1800):
 p=b/'dify_compose_20261118/run_cached.log';s=p.read_text(errors='replace')
 if 'ONLY_THIS_TEST_COMPOSE_STOPPED_DATA_PRESERVED' in s or 'Traceback' in s:break
 time.sleep(1)
else:raise TimeoutError('Dify deployment still busy; do not benchmark concurrently')
for _ in range(1200):
 p=b/'dify_thin_runtime_20261118/run_v2.log';text=p.read_text(errors='replace') if p.exists() else ''
 if 'THIN_TEST_STOPPED_DATA_PRESERVED' in text or 'Traceback' in text:break
 time.sleep(1)
else:raise TimeoutError('Thin Dify runtime busy; postpone performance timing')
def semantics(c,label):
 checks=["select count(*) from docs where match(title_en,content_en) against('database audit')","select count(*) from docs where not match(title_en,content_en) against('database audit')","select sum(match(title_en,content_en) against('database audit')) from docs","select count(*) from docs where match(title_en,content_en) against('database audit') between0.01 and5".replace('between0','between 0').replace('and5','and 5'),"select count(*) from docs where match(title_en,content_en) against('database audit') or id<10","select id%5,count(*) from docs where match(title_en,content_en) against('database audit') group by id%5 order by id%5","select count(*) from docs where match(title_en,content_en) against('database audit') and match(title_en,content_en) against('database audit')","select count(*) from docs where match(title_en,content_en) against('')","select count(*) from docs where match(title_en,content_en) against(NULL)","select count(*) from docs where match(title_en,content_en) against('database audit')>0","select count(*) from docs where match(title_en,content_en) against('database audit')<0.5","select id,match(title_en,content_en) against('database audit') as s from docs where match(title_en,content_en) against('database audit') order by s desc,id limit20".replace('limit20','limit 20')]
 values=[]
 for q in checks:c.execute(q);values.append(c.fetchall())
 c.execute('create database if not exists fts_filter_regression');c.execute('drop table if exists fts_filter_regression.docs');c.execute('create table fts_filter_regression.docs(id int primary key,body text,fulltext index ft(body) with parser beng)');c.executemany('insert into fts_filter_regression.docs values(%s,%s)',[(1,'alpha beta alpha'),(2,'beta audit'),(3,'alpha audit')])
 for action,count in [(None,3),('begin',3),("update fts_filter_regression.docs set body='other' where id=1",2),('rollback',3),('delete from fts_filter_regression.docs where id=3',2)]:
  if action:c.execute(action)
  c.execute("select count(*) from fts_filter_regression.docs where match(body) against('alpha beta')");assert c.fetchone()[0]==count,(label,action,count)
 c.execute('drop database fts_filter_regression');return json.loads(json.dumps(values,default=str))
results=[];reference=None
for phase,label in enumerate(['warmup','before','after','after','before']):
 binary=b/'fts_parallel_build/seekdb_candidate' if label in ['before','warmup'] else logs/'seekdb_candidate';name='fts-count-final-'+str(phase)+'-20261118';tag=label+'_container_phase'+str(phase)
 cmd=['docker','run','-d','--name',name,'--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','512','-p','127.0.0.1:12890:2881','-v',str(data)+':/data','-v',str(binary)+':/srv/seekdb:ro','-v',str(root/'deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so.1')+':/srv/lib/libaio.so.1:ro','-v',str(root/'deps/3rd/u01/obclient')+':/client:ro','-v',str(root/'tools/benchmark')+':/benchmark:ro','-v',str(logs)+':/evidence','-e','LD_LIBRARY_PATH=/srv/lib:/client/lib','--entrypoint','/srv/seekdb','dream-dify-root:20261117','--base-dir=/data','--port=2881','--nodaemon','--parameter','cpu_count=2','--parameter','log_disk_size=2G']
 subprocess.run(cmd,env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(180):
   try:con=pymysql.connect(host='127.0.0.1',port=12890,user='root',autocommit=True,charset='utf8mb4',read_timeout=120);con.close();break
   except pymysql.Error:time.sleep(.5)
  else:raise TimeoutError('Paired container SQL unavailable')
  client='/client/bin/obclient -h127.0.0.1 -P2881 -uroot -N -s -A --default-character-set=utf8mb4'
  args=['docker','exec','-e','MYSQL='+client,'-e','MYSQL_VERBOSE='+client.replace('-N -s ',''),'-e','LABEL='+tag,'-e','ROWS=20000','-e','BATCH=500','-e','ROUNDS=3000','-e','QUERY_ROUNDS=200','-e','SAMPLES=3','-e','WARMUP=30','-e','SKIP_LOAD=0','-e','OUTPUT=/evidence/'+tag+'_report.txt',name,'bash','/benchmark/fts_large_bench.sh']
  with (logs/(tag+'.log')).open('wb') as out:r=subprocess.run(args,env=env,stdout=out,stderr=out,timeout=600)
  assert r.returncode==0,(tag,r.returncode)
  con=pymysql.connect(host='127.0.0.1',port=12890,user='root',database='fts_large_bench',autocommit=True,charset='utf8mb4',read_timeout=120)
  with con.cursor() as cur:answers=semantics(cur,tag)
  con.close()
  if reference is None:reference=answers
  else:assert reference==answers,'SQL score/filter/group/NULL mismatch'
  info=json.loads(subprocess.check_output(['docker','inspect',name],env=env))[0];assert not info['State']['OOMKilled']
  workers=subprocess.check_output(['docker','exec',name,'sh','-c','cat /proc/1/task/*/comm'],env=env,text=True).splitlines();workers=sum(n.startswith('DdlBuild') for n in workers)
  metrics={}
  for line in (logs/(tag+'_report.txt')).read_text().splitlines():
   m=re.match(r'\s*([a-z0-9_]+):\s*([-+.0-9]+)\s*$',line)
   if m:metrics[m[1]]=float(m[2])
  records={'phase':phase,'variant':label,'metrics':metrics,'actual_memory_limit_bytes':info['HostConfig']['Memory'],'actual_cpu_quota':info['HostConfig']['NanoCpus'],'actual_DDL_workers':workers,'OOMKilled':False};results.append(records);(logs/'actual_container_abba.json').write_text(json.dumps(results,indent=2));print('REAL_2CPU_2G_FTS_ABBA_PHASE_PASS',tag,metrics['build_total_sec'],'workers',workers,flush=True)
 finally:subprocess.run(['docker','stop','--time','30',name],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
keys=['build_ik_all_sec','build_ik_content_sec','build_beng_en_sec','query_cn_avg_ms','query_beng_avg_ms','query_mixed_avg_ms','query_limit_avg_ms','tokenize_ik_avg_ms','tokenize_beng_avg_ms']
means={label:{k:statistics.mean(x['metrics'][k] for x in results if x['variant']==label) for k in keys} for label in ['before','after']}
report={'status':'PASS','same_actual_cgroup_resources':'2CPU2GiB512PID','initial_full_warmup_excluded_from_means':True,'phases':results,'mean_metrics':means,'speedup_by_metric':{k:means['before'][k]/means['after'][k] for k in keys},'semantic_scores_filters_NULL_group_duplicateMATCH':'PASS','DML_rollback':'PASS','official_benchmark_unchanged':True}
(logs/'actual_container_abba_summary.json').write_text(json.dumps(report,indent=2));print('ACTUAL_CGROUP_FULL_FTS_ABBA_COMPLETE',json.dumps(report),flush=True)
