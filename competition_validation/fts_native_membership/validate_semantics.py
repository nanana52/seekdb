
from pathlib import Path
import os,signal,subprocess,json,time,sys,pymysql
base=Path('/opt/oceanbase_completion_20261116');root=base/'seekdb';runtime=base/'seekdb_runtime_abi_fixed';logs=Path(sys.argv[1]);logs.mkdir(exist_ok=True)
queries=[
"select count(*) from docs where match(title_en,content_en) against('database performance audit')",
"select count(*) from docs where match(title,content) against('OceanBase database')",
"select count(*) from docs where match(content) against('tokenizer')",
"select count(*) from docs where match(content) against('no_such_word_zzz')",
"select id,match(title_en,content_en) against('database audit') as s from docs where match(title_en,content_en) against('database audit') order by s desc,id limit 20",
"select id from docs where match(content) against('tokenizer') limit 20",
"select count(*) from docs where match(title_en,content_en) against('database audit')>1.0",
"select count(*) from docs where match(title_en,content_en) against('+database -audit' in boolean mode)",
"select count(*) from docs where match(title_en,content_en) against('database audit') and id%2=0",
]
reference={}
def stop():
 p=runtime/'pid'
 if p.exists():
  pid=int(p.read_text());cmd=Path('/proc')/str(pid)/'cmdline'
  if cmd.exists() and str(base).encode() in cmd.read_bytes():
   os.kill(pid,signal.SIGTERM)
   for _ in range(100):
    if not cmd.exists() or (cmd.parent/'stat').read_text().split()[2]=='Z':break
    time.sleep(.1)
def start(binary):
 with (runtime/'stdout.log').open('ab') as out:p=subprocess.Popen([str(binary),'--base-dir='+str(runtime),'--port=12881','--nodaemon','--parameter','memory_limit=2G','--parameter','log_disk_size=2G'],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
 (runtime/'pid').write_text(str(p.pid))
 for _ in range(90):
  try:return pymysql.connect(host='127.0.0.1',port=12881,user='root',database='fts_large_bench',charset='utf8mb4',autocommit=True)
  except Exception:
   if p.poll() is not None:raise RuntimeError('FTS instance exited')
   time.sleep(.3)
 raise TimeoutError('FTS start')

queries += [
 "select count(*) from docs where not match(title_en,content_en) against('database audit')",
 "select sum(match(title_en,content_en) against('database audit')) from docs",
 "select count(*) from docs where match(title_en,content_en) against('database audit') between 0.01 and 5",
 "select count(*) from docs where match(title_en,content_en) against('database audit') or id<10",
 "select id%5,count(*) from docs where match(title_en,content_en) against('database audit') group by id%5 order by id%5",
 "select count(*) from docs where match(title_en,content_en) against('database audit') and match(title_en,content_en) against('database audit')",
 "select count(*) from docs where match(title_en,content_en) against('')",
 "select count(*) from docs where match(title_en,content_en) against(NULL)",
 "select count(*) from docs where match(title_en,content_en) against('database audit')>0",
 "select count(*) from docs where match(title_en,content_en) against('database audit')<0.5",
 "select count(*) from docs where match(title_en,content_en) against(' '.join([]))",
]
queries=queries[:-1]
for label,binary in [('before',base/'fts_before_score_prune'),('after',root/'build_release/src/observer/seekdb')]:
 stop();con=start(binary)
 with con.cursor() as c:
  for i,query in enumerate(queries):
   c.execute(query);answer=c.fetchall()
   if label=='before':reference[i]=answer
   else:assert answer==reference[i],(query,answer,reference[i])
  c.execute('create database if not exists fts_membership_dml')
  c.execute('drop table if exists fts_membership_dml.docs')
  c.execute('create table fts_membership_dml.docs(id int primary key, body text, fulltext index ft(body) with parser beng)')
  c.executemany('insert into fts_membership_dml.docs values(%s,%s)',[(1,'Alpha beta alpha'),(2,'beta audit'),(3,'alpha audit')])
  for step,action,expected in [(0,None,2),(1,'start transaction',2),(2,"update fts_membership_dml.docs set body='other' where id=1",1),(3,'rollback',2),(4,'delete from fts_membership_dml.docs where id=3',1)]:
   if action:c.execute(action)
   c.execute("select count(*) from fts_membership_dml.docs where match(body) against('alpha')");assert c.fetchone()[0]==expected,(label,step)
  c.execute('drop database fts_membership_dml')
 print('NATIVE_FTS_SEMANTICS_PASS',label,len(queries),'queries plus DML/rollback',flush=True)
 con.close()
print('NATIVE_FTS_SCORES_THRESHOLDS_BOOLEAN_NULL_GROUPS_ROLLBACK_PASS',flush=True)
