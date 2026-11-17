from pathlib import Path
import os,signal,time,json,pymysql,subprocess
b=Path('/opt/oceanbase_completion_20261116');runtime=b/'ai_column_strict_retry_20261117';logs=b/'fts_binary_compare'
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
queries=[]
for text in ['database performance audit index storage vector memory query','database audit missingword tokenizer','performance performance audit database','+database +audit -performance']:
 expr="match(title_en,content_en) against("+repr(text)+(" in boolean mode)" if text.startswith('+') else ')')
 for select in ['count(*)','sum('+expr+')','min('+expr+'),max('+expr+')']:
  queries.append('select '+select+' from docs where '+expr)
 queries.extend(['select id,'+expr+' as score from docs where '+expr+' order by score desc,id limit10 offset7'.replace('limit10','limit 10').replace('offset7','offset 7'),'select count(*) from docs where ('+expr+')>0.5','select count(*) from docs where not ('+expr+')','select id%11,count(*) from docs where '+expr+' group by id%11 order by id%11'])
reference=None
for label,binary in [('before',b/'fts_parallel_build/seekdb_candidate'),('after',logs/'seekdb_candidate')]:
 con=start(binary);values=[]
 with con.cursor() as cur:
  for q in queries:
   cur.execute(q);values.append(cur.fetchall())
 con.close()
 if reference is None:reference=values
 else:
  for i,(a,z) in enumerate(zip(reference,values)):assert a==z,(i,queries[i],a,z)
 print('LARGE_FANIN_SCORE_BOOLEAN_OFFSET_GROUP_PASS',label,len(queries),flush=True)
report={'status':'PASS','queries':len(queries),'scope':'1..8term textDaaT fanin including generic loser-tree fallback; exact score minmaxsum; booleanmustprohibit; ranktiesID; LIMITOFFSET; scorethreshold; NOT; group'};(logs/'fanin_score_semantics.json').write_text(json.dumps(report,indent=2));print('FULL_FANIN_SCORE_REGRESSION_PASS',json.dumps(report),flush=True)
