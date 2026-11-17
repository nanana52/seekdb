from pathlib import Path
import os,subprocess,time,json,shutil,pymysql
b=Path('/opt/oceanbase_completion_20261116');p=b/'fts_aux_pipeline';env=dict(os.environ,DOCKER_HOST='unix://'+str(b/'docker/docker.sock'))
for _ in range(1200):
 text=(p/'semantics.log').read_text(errors='replace')
 if 'ADAPTIVE_DDL_FULL_SEMANTICS_PASS' in text:break
 if 'Traceback' in text:raise RuntimeError(text[-1600:])
 time.sleep(1)
else:raise TimeoutError('Adaptive performance timing pending')
records=[]
for mem,cpus,wanted in [('2g',1,1),('1536m',4,1),('2g',2,2),('2g',4,2)]:
 tag=mem+'-'+str(cpus);data=p/('container-guards-retry-'+tag);data.mkdir(exist_ok=False);name='fts-build-guard-'+tag+'-aux-pipeline-20261118'
 cmd=['docker','run','-d','--name',name,'--cpus',str(cpus),'--memory',mem,'--memory-swap',mem,'--pids-limit','512','-p','127.0.0.1:12890:2881','-v',str(data)+':/data','-v',str(p/'seekdb_candidate')+':/srv/seekdb:ro','-v',str(b/'seekdb/deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so.1')+':/srv/lib/libaio.so.1:ro','-e','LD_LIBRARY_PATH=/srv/lib','--entrypoint','/srv/seekdb','dream-dify-root:20261117','--base-dir=/data','--port=2881','--nodaemon','--parameter','cpu_count='+str(cpus),'--parameter','log_disk_size=2G']
 subprocess.run(cmd,env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 start=time.monotonic()
 try:
  for _ in range(180):
   try:c=pymysql.connect(host='127.0.0.1',port=12890,user='root',charset='utf8mb4',autocommit=True,read_timeout=120);break
   except pymysql.Error:time.sleep(.5)
  else:raise TimeoutError('Restricted seekdb instance failed')
  tasknames=subprocess.check_output(['docker','exec',name,'sh','-c','cat /proc/1/task/*/comm'],env=env,text=True).splitlines();workers=sum(n.startswith('DdlBuild') for n in tasknames)
  assert workers==wanted,(tag,workers,wanted)
  with c.cursor() as cur:
   cur.execute('create database verifyddl');cur.execute('create table verifyddl.docs(id bigint primary key,k bigint,body text)')
   cur.executemany('insert into verifyddl.docs values(%s,%s,%s)',[(i,i+100,'alpha beta' if i%2 else 'beta gamma') for i in range(1,101)])
   cur.execute('create index by_k on verifyddl.docs(k)');cur.execute('create unique index uq_k on verifyddl.docs(k)')
   cur.execute('create fulltext index ft_body on verifyddl.docs(body) with parser beng')
   cur.execute("select count(*) from verifyddl.docs where match(body) against('alpha')");assert cur.fetchone()[0]==50
   try:cur.execute("insert into verifyddl.docs values(999,101,'other')");raise AssertionError('Unique index not enforced')
   except pymysql.IntegrityError:pass
   cur.execute('begin');cur.execute('delete from verifyddl.docs where id<20');cur.execute('rollback')
   cur.execute('select count(*) from verifyddl.docs force index(by_k) where k between101 and150'.replace('between101','between 101').replace('and150','and 150'));assert cur.fetchone()[0]==50
  c.close()
  logs=(data/'log/seekdb.log').read_text(errors='replace')
  ddl_lines=[line for line in logs.splitlines() if 'execute sql' in line and 'ft_body' in line and 'INSERT' in line]
  expected_dop=2 if cpus>=4 and mem=='2g' else 1
  assert any('parallel('+str(expected_dop)+')' in line for line in ddl_lines),(tag,'Auxiliary DDL DOP not observed',ddl_lines[-2:])
  info=json.loads(subprocess.check_output(['docker','inspect',name],env=env))[0];assert not info['State']['OOMKilled']
  peakfile='memory.peak';peak=subprocess.run(['docker','exec',name,'cat','/sys/fs/cgroup/'+peakfile],env=env,capture_output=True,text=True).stdout.strip()
  row={'memory':mem,'configured_cpu_count':cpus,'actual_cpu_quota':info['HostConfig']['NanoCpus'],'memory_limit_bytes':info['HostConfig']['Memory'],'actual_ddl_workers':workers,'observed_fulltext_DOP':expected_dop,'normal_unique_fulltext_rollback':'PASS','OOMKilled':False,'cgroup_memory_peak_bytes':peak,'seconds':time.monotonic()-start};records.append(row);(p/'actual_container_guards.json').write_text(json.dumps({'status':'PASS','profiles':records},indent=2));print('REAL_CONTAINER_DDL_RESOURCE_GUARD_PASS',json.dumps(row),flush=True)
 finally:subprocess.run(['docker','stop','--time','30',name],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
print('ALL_REAL_CGROUP_GUARDS_AND_SHARED_DDL_PASS',flush=True)
