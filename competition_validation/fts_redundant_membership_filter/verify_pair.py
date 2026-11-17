from pathlib import Path
import os,signal,time,subprocess,json,shutil,statistics,pymysql
b=Path('/opt/oceanbase_completion_20261116');root=b/'seekdb';logs=b/'fts_membership_filter';runtime=b/'ai_column_strict_retry_20261117';candidate=logs/'seekdb_candidate'
with (root/'build_release/src/observer/seekdb').open('rb') as i,candidate.open('wb') as o:shutil.copyfileobj(i,o,1024*1024)
candidate.chmod(0o755)
queries=["select count(*) from docs where match(title,content) against('\u5168\u6587\u7d22\u5f15 \u5206\u8bcd\u5668')","select count(*) from docs where match(title_en,content_en) against('database performance audit')","select count(*) from docs where match(title,content) against('OceanBase \u7a33\u5b9a database')","select count(*) from (select id from docs where match(content) against('\u5012\u6392\u7d22\u5f15 tokenizer') limit20) t".replace('limit20','limit 20')]
expected=[8001,11000,7332,20]
records=[];references={}
def start(binary):
 old=Path('/proc')/(runtime/'pid').read_text().strip()
 if old.exists() and (old/'stat').read_text().split()[2]!='Z':
  assert ('--base-dir='+str(runtime)).encode() in (old/'cmdline').read_bytes();os.kill(int(old.name),signal.SIGTERM)
  for _ in range(100):
   if not old.exists() or (old/'stat').read_text().split()[2]=='Z':break
   time.sleep(.2)
  else:raise RuntimeError('Isolated instance did not stop')
 with (runtime/'stdout.log').open('ab') as out:p=subprocess.Popen([str(binary),'--base-dir='+str(runtime),'--port=12889','--nodaemon','--parameter','memory_limit=2G','--parameter','log_disk_size=2G'],stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
 (runtime/'pid').write_text(str(p.pid))
 for _ in range(100):
  try:return pymysql.connect(host='127.0.0.1',port=12889,user='root',database='fts_large_bench',autocommit=True,charset='utf8mb4',read_timeout=60)
  except pymysql.Error:
   if p.poll() is not None:raise RuntimeError('Instance exited')
   time.sleep(.3)
 raise TimeoutError('Instance did not become ready')
def semantics(c,label):
 checks=["select count(*) from docs where match(title_en,content_en) against('database audit')","select count(*) from docs where not match(title_en,content_en) against('database audit')","select sum(match(title_en,content_en) against('database audit')) from docs","select count(*) from docs where match(title_en,content_en) against('database audit') between0.01 and5".replace('between0','between 0').replace('and5','and 5'),"select count(*) from docs where match(title_en,content_en) against('database audit') or id<10","select id%5,count(*) from docs where match(title_en,content_en) against('database audit') group by id%5 order by id%5","select count(*) from docs where match(title_en,content_en) against('database audit') and match(title_en,content_en) against('database audit')","select count(*) from docs where match(title_en,content_en) against('')","select count(*) from docs where match(title_en,content_en) against(NULL)","select count(*) from docs where match(title_en,content_en) against('database audit')>0","select count(*) from docs where match(title_en,content_en) against('database audit')<0.5","select id,match(title_en,content_en) against('database audit') as s from docs where match(title_en,content_en) against('database audit') order by s desc,id limit20".replace('limit20','limit 20')]
 values=[]
 for q in checks:c.execute(q);values.append(c.fetchall())
 c.execute('create database if not exists fts_filter_regression');c.execute('drop table if exists fts_filter_regression.docs');c.execute('create table fts_filter_regression.docs(id int primary key,body text,fulltext index ft(body) with parser beng)');c.executemany('insert into fts_filter_regression.docs values(%s,%s)',[(1,'alpha beta alpha'),(2,'beta audit'),(3,'alpha audit')])
 for action,count in [(None,3),('begin',3),("update fts_filter_regression.docs set body='other' where id=1",2),('rollback',3),('delete from fts_filter_regression.docs where id=3',2)]:
  if action:c.execute(action)
  c.execute("select count(*) from fts_filter_regression.docs where match(body) against('alpha beta')");assert c.fetchone()[0]==count,(label,action,count)
 c.execute('drop database fts_filter_regression');return json.loads(json.dumps(values,default=str))
for phase,label in enumerate(['before','after','after','before']):
 con=start(b/'fts_count_only/seekdb_candidate' if label=='before' else candidate)
 with con.cursor() as c:
  vals=semantics(c,label)
  if phase==0:references['semantics']=vals
  else:assert vals==references['semantics'],label
  for q,wanted in zip(queries,expected):c.execute(q);assert c.fetchone()[0]==wanted
  samples=[]
  for _ in range(3):
   timing=[]
   for q in queries:
    for _ in range(30):c.execute(q);c.fetchall()
    at=time.perf_counter()
    for _ in range(200):c.execute(q);c.fetchall()
    timing.append((time.perf_counter()-at)*1000/200)
   samples.append(timing)
 con.close();row={'phase':phase,'variant':label,'latency_ms_samples':samples};records.append(row);(logs/'query_pair.json').write_text(json.dumps(records,indent=2));print(json.dumps(row),flush=True)
summary={}
for label in ['before','after']:
 vals=[s for rec in records if rec['variant']==label for s in rec['latency_ms_samples']];summary[label]=[statistics.median([s[i] for s in vals]) for i in range(4)]
ratio=[a/z for a,z in zip(summary['before'],summary['after'])];report={'status':'PASS','scope':'ABBA alternating actual query benchmark with warmups200rounds3samplesperphase; same20Kdataset','sql_hits':expected,'latency_ms_median':summary,'speedup_by_query':ratio,'semantic_reference':references,'dml_and_rollback':'PASS'};(logs/'query_pair_summary.json').write_text(json.dumps(report,indent=2));print('FTS_MEMBERSHIP_FILTER_PAIRED_PASS',json.dumps(report),flush=True)
