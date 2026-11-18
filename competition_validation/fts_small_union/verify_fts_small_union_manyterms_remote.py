from pathlib import Path
import time,subprocess,os,json,pymysql
b=Path('/opt/oceanbase_completion_20261116');p=b/'fts_small_union'
for _ in range(1200):
 s=(p/'actual_cgroup_abba.log').read_text(errors='replace')
 if 'ACTUAL_CGROUP_FULL_FTS_ABBA_COMPLETE' in s:break
 if 'Traceback' in s:raise RuntimeError(s[-1800:])
 time.sleep(1)
else:raise TimeoutError('Do not interfere with timing')
env=dict(os.environ,DOCKER_HOST='unix://'+str(b/'docker/docker.sock'));result=[];reference=None
for variant,bin in [('before',b/'fts_count_hotloop/seekdb_candidate'),('after',p/'seekdb_candidate')]:
 name='fts-smallunion-manyterms-'+variant+'-retry-20261118';data=p/('manyterm-retry-'+variant);data.mkdir()
 subprocess.run(['docker','run','-d','--name',name,'--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','512','-p','127.0.0.1:12890:2881','-v',str(data)+':/data','-v',str(bin)+':/srv/seekdb:ro','-v',str(b/'seekdb/deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so.1')+':/srv/lib/libaio.so.1:ro','-e','LD_LIBRARY_PATH=/srv/lib','--entrypoint','/srv/seekdb','dream-dify-root:20261117','--base-dir=/data','--port=2881','--nodaemon','--parameter','cpu_count=2','--parameter','log_disk_size=2G'],env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(200):
   try:c=pymysql.connect(host='127.0.0.1',port=12890,user='root',autocommit=True,charset='utf8mb4',read_timeout=120);break
   except pymysql.Error:time.sleep(.4)
  else:raise TimeoutError('Manytermenvstartup')
  words='alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mercury venus earth mars'.split()
  answers=[]
  with c.cursor() as cur:
   cur.execute('create database multimerge');cur.execute('create table multimerge.docs(id int primary key,body text)')
   rows=[(i,None if i%31==0 else ' '.join(w for j,w in enumerate(words) if (i*(j+1)+j*j)%7<3)) for i in range(1,601)]
   cur.executemany('insert into multimerge.docs values(%s,%s)',rows);cur.execute('create fulltext index ft on multimerge.docs(body) with parser beng')
   for phase in ['base','updated','rollback']:
    if phase=='updated':cur.execute('begin');cur.execute("update multimerge.docs set body='alpha theta mercury' where id%7=0");cur.execute('delete from multimerge.docs where id%19=0')
    if phase=='rollback':cur.execute('rollback')
    for n in range(1,16):
     terms=' '.join(words[:n]);duplicate=terms+' '+words[0]+' missingterm'
     for expr,sqltail in [('count(*)',''),('count(1)',''),('sum(1)',''),('count(*)',' and id%%3=1')]:
      for query in [terms,duplicate]:
       cur.execute('select '+expr+' from multimerge.docs where match(body) against(%s)'+sqltail,(query,));answers.append(cur.fetchall())
     cur.execute('select count(*) from (select 1 from multimerge.docs where match(body) against(%s) limit 3 offset 7)t',(terms,));answers.append(cur.fetchall())
  c.close();answers=json.loads(json.dumps(answers,default=str));(p/('manyterm_'+variant+'.json')).write_text(json.dumps(answers))
  if reference is None:reference=answers
  else:assert reference==answers,'Manytermsemanticsdifference'
  result.append({'variant':variant,'checks':len(answers)});print('MANYTERM_ADVERSARIAL_PASS',variant,len(answers),flush=True)
 finally:subprocess.run(['docker','stop','--time','30',name],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
(p/'manyterm_result.json').write_text(json.dumps({'status':'PASS','scope':'1to15terms;duplicatesunknown;COUNT/SUM;offsetlimit;extraSQLfilter;DMLrollback;comparebeforeafteractual2CPU2GiB','phases':result},indent=2));print('SMALLUNION_ADVERSARIAL_SEMANTICS_PASS',flush=True)
