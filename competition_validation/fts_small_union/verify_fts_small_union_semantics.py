from pathlib import Path
import os,signal,time,subprocess,json,pymysql,random
b=Path('/opt/oceanbase_completion_20261116');logs=b/'fts_small_union';runtime=b/'ai_column_strict_retry_20261117'
for _ in range(2000):
 s=(logs/'run.log').read_text(errors='replace')
 if 'COMPACT_DAT_SEMANTICS_AND_COMPLETE_BENCH_PASS' in s:break
 if 'Traceback' in s:raise RuntimeError(s[-1600:])
 time.sleep(1)
else:raise TimeoutError('COUNT cardinality build pending')
def start(binary):
 old=Path('/proc')/(runtime/'pid').read_text().strip()
 if old.exists() and (old/'stat').read_text().split()[2]!='Z':
  assert ('--base-dir='+str(runtime)).encode() in (old/'cmdline').read_bytes();os.kill(int(old.name),signal.SIGTERM)
  for _ in range(100):
   if not old.exists() or (old/'stat').read_text().split()[2]=='Z':break
   time.sleep(.2)
  else:raise RuntimeError('Isolated instance did not stop')
 for lockfile in [runtime/'run/seekdb.pid']:
  if lockfile.exists():
   recorded=lockfile.read_text().strip();proc=Path('/proc')/recorded
   if not proc.exists() or (proc/'stat').read_text().split()[2]=='Z':lockfile.unlink()
 with (runtime/'stdout.log').open('ab') as out:p=subprocess.Popen([str(binary),'--base-dir='+str(runtime),'--port=12889','--nodaemon','--parameter','memory_limit=2G','--parameter','log_disk_size=2G'],stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
 (runtime/'pid').write_text(str(p.pid))
 for _ in range(100):
  try:return pymysql.connect(host='127.0.0.1',port=12889,user='root',database='fts_large_bench',autocommit=True,charset='utf8mb4',read_timeout=60)
  except pymysql.Error:
   if p.poll() is not None:raise RuntimeError('Instance exited')
   time.sleep(.3)
 raise TimeoutError('Instance did not become ready')
terms=['database performance audit','database audit','unseenword','',None]
queries=[]
for term in terms:
 literal='NULL' if term is None else "'"+term+"'"
 expr='match(title_en,content_en) against('+literal+')'
 queries.extend(['select count(*) from docs where '+expr,
                 'select count(1),sum(1) from docs where '+expr,
                 'select count(id),sum(id),min(id),max(id) from docs where '+expr,
                 'select 7 from docs where '+expr+' limit5 offset3'.replace('limit5','limit 5').replace('offset3','offset 3'),
                 'select count(*) from docs where '+expr+' and id%3=0',
                 'select id%7,count(*) from docs where '+expr+' group by id%7 order by id%7',
                 'select count(*) from (select 1 from docs where '+expr+' limit20 offset5) s'.replace('limit20','limit 20').replace('offset5','offset 5')])
queries.extend(["select count(*) from docs where match(title_en,content_en) against('+database -audit' in boolean mode)",
                "select count(*) from docs where match(title_en,content_en) against('database audit')>0.3",
                "select count(*) from docs where not match(title_en,content_en) against('database audit')",
                "select count(*) from docs where match(title_en,content_en) against('database') and match(title_en,content_en) against('audit')"])
reference=None
for label,binary in [('before',b/'fts_parallel_build/seekdb_candidate'),('after',logs/'seekdb_candidate')]:
 con=start(binary);answers=[]
 with con.cursor() as c:
  for query in queries:c.execute(query);answers.append(c.fetchall())
 con.close()
 if reference is None:reference=answers
 else:
  for i,(a,z) in enumerate(zip(reference,answers)):assert a==z,(i,queries[i],a,z)
 print('ADAPTIVE_DDL_SEMANTICS_PASS',label,len(queries),flush=True)
oracle='from pathlib import Path\nimport random,json,pymysql\nb=Path(\'/opt/oceanbase_completion_20261116\');root=b/\'seekdb\';logs=b/\'fts_small_union\'\ncon=pymysql.connect(host=\'127.0.0.1\',port=12889,user=\'root\',autocommit=True,charset=\'utf8mb4\',read_timeout=120)\nrng=random.Random(20261118);vocab=[\'alpha\',\'beta\',\'gamma\',\'delta\',\'database\',\'performance\',\'audit\',\'tokenizer\',\'oceanbase\',\'memory\',\'transaction\']\ndocs={}\nfor i in range(1,141):\n text=None if i%23==0 else \'\' if i%19==0 else \' \'.join(rng.choice(vocab) for _ in range([1,3,9,30,200,1200,3][i%7]))\n if text and i%2==0:text=text.upper()\n docs[i]=text\ndef ids(cur,terms):\n cur.execute("select id from wordmap_reuse_dml.docs where match(body) against(%s) order by id",(\' \'.join(terms),))\n return [x[0] for x in cur.fetchall()]\ndef expected(terms):\n return sorted(i for i,text in docs.items() if text and any(t in text.lower().split() for t in terms))\nchecks=0\nwith con.cursor() as c:\n c.execute(\'create database if not exists wordmap_reuse_dml\');c.execute(\'drop table if exists wordmap_reuse_dml.docs\');c.execute(\'create table wordmap_reuse_dml.docs(id bigint primary key,body text)\')\n c.executemany(\'insert into wordmap_reuse_dml.docs values(%s,%s)\',list(docs.items()))\n c.execute(\'alter table wordmap_reuse_dml.docs add fulltext index ft(body) with parser beng\')\n queries=[rng.sample(vocab,rng.randint(1,5)) for _ in range(40)]+[[\'notpresent\'],[\'alpha\'],[\'alpha\',\'alpha\']]\n for terms in queries:assert ids(c,terms)==expected(terms),terms;checks+=1\n c.execute(\'begin\');c.execute("update wordmap_reuse_dml.docs set body=\'alpha\' where id between1 and20".replace(\'between1\',\'between 1\').replace(\'and20\',\'and 20\'));c.execute(\'rollback\')\n for terms in queries[:10]:assert ids(c,terms)==expected(terms),terms;checks+=1\n for i in [1,7,50,139]:\n  text=\' \'.join([\'alpha\',\'tokenizer\']*(i%3+1));c.execute(\'update wordmap_reuse_dml.docs set body=%s where id=%s\',(text,i));docs[i]=text\n c.execute(\'delete from wordmap_reuse_dml.docs where id%7=0\');docs={i:t for i,t in docs.items() if i%7}\n for terms in queries:assert ids(c,terms)==expected(terms),terms;checks+=1\n c.execute(\'drop database wordmap_reuse_dml\')\nreport={\'status\':\'PASS\',\'checks\':checks,\'docs\':140,\'scope\':\'Independent Python token-set oracle for actual BENG bulk index; varying text1..1200words; caseemptyNULL; update/delete; transaction rollback\'}\n(logs/\'dml_oracle.json\').write_text(json.dumps(report,indent=2));print(\'WORDMAP_REUSE_DML_ORACLE_PASS\',json.dumps(report),flush=True)\n'
(logs/'dml_oracle.py').write_text(oracle);r=subprocess.run([str(b/'venv/bin/python'),str(logs/'dml_oracle.py')]);assert r.returncode==0
report={'status':'PASS','queries_per_variant':len(queries),'scope':'COUNTstarCOUNT1SUM1projectionconstantLIMITOFFSETemptyNULLgroupnumericprojectionextrafilterbooleanNOTdoubleMATCH; exactbeforeafteranswers','DML_oracle':json.loads((logs/'dml_oracle.json').read_text())}
(logs/'cardinality_semantics.json').write_text(json.dumps(report,indent=2));print('ADAPTIVE_DDL_FULL_SEMANTICS_PASS',json.dumps(report),flush=True)
