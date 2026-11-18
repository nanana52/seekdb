from pathlib import Path
import os,signal,time,subprocess,json,shutil,pymysql
b=Path('/opt/oceanbase_completion_20261116');root=b/'seekdb';logs=b/'fts_small_union';runtime=b/'ai_column_strict_retry_20261117';logs.mkdir(exist_ok=True)
def start(binary):
 old=Path('/proc')/(runtime/'pid').read_text().strip()
 if old.exists() and (old/'stat').read_text().split()[2]!='Z':
  assert ('--base-dir='+str(runtime)).encode() in (old/'cmdline').read_bytes();os.kill(int(old.name),signal.SIGTERM)
  for _ in range(200):
   if not old.exists() or (old/'stat').read_text().split()[2]=='Z':break
   time.sleep(.1)
  else:raise RuntimeError('Instance stop failed')
 lock=runtime/'run/seekdb.pid'
 if lock.exists():
  owner=Path('/proc')/lock.read_text().strip()
  if not owner.exists() or (owner/'stat').read_text().split()[2]=='Z':lock.unlink()
 with (runtime/'stdout.log').open('ab') as out:p=subprocess.Popen([str(binary),'--base-dir='+str(runtime),'--port=12889','--nodaemon','--parameter','memory_limit=2G','--parameter','log_disk_size=2G'],stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
 (runtime/'pid').write_text(str(p.pid))
 for _ in range(150):
  try:
   con=pymysql.connect(host='127.0.0.1',port=12889,user='root',autocommit=True);con.close();return
  except pymysql.Error:
   if p.poll() is not None:raise RuntimeError('Instance exited')
   time.sleep(.3)
 raise TimeoutError('Instance start failed')
verify="from pathlib import Path\nimport json,os,random,subprocess,sys,time\nimport pymysql\nbase=Path('/opt/oceanbase_completion_20261116');root=base/'seekdb'\nlogs=base/'fts_small_union';logs.mkdir(exist_ok=True)\nlabel=sys.argv[1]\ncon=pymysql.connect(host='127.0.0.1',port=12889,user='root',database='test',charset='utf8mb4',autocommit=True,read_timeout=60)\ncases=json.loads((base/'fts_tokenize_before.json').read_text())\nqueries=[]\nfor rec in cases:queries.append(('select tokenize(%s,%s) as tokens',(rec['text'],rec['parser'])))\nrng=random.Random(20261117)\nfor coll in ['utf8mb4_bin','utf8mb4_general_ci','utf8mb4_unicode_ci','utf8mb4_turkish_ci']:\n    for _ in range(40):\n        text=''.join(chr(rng.randrange(32,127)) for _ in range(70))+' I i ? ? ? ? ?? U0001f600'\n        for parser in ['BENG','IK']:\n            queries.append(('select tokenize(convert(%s using utf8mb4) collate '+coll+',%s) as tokens',(text,parser)))\nfor parser in ['BENG','SPACE','NGRAM','NGRAM2','IK']:\n    for text in ['one TWO alpha beta','I \\u0130 i \\u00df \\u4e2d\\u6587','one two one','',None]:\n        for options in [[],[{'output':'all'}],[{'additional_args':['min_token_size=1','max_token_size=20']}],[{'additional_args':['ngram_token_size=3']}],[{'additional_args':['ik_mode=smart']}],[{'unknown':'invalid'}]]:\n            queries.append(('select tokenize(%s,%s,cast(%s as json)) as tokens',(text,parser,json.dumps(options))))\nanswers=[]\nwith con.cursor() as c:\n    for sql,args in queries:\n        try:\n            c.execute(sql,args);value=json.loads(c.fetchone()[0]);value=sorted(value,key=lambda x:json.dumps(x,sort_keys=True,ensure_ascii=False)) if isinstance(value,list) else value\n            answers.append({'value':value})\n        except pymysql.Error as e:answers.append({'error':e.args[0]})\ncon.close()\nif label=='before':\n    (logs/'token_reference.json').write_text(json.dumps(answers,ensure_ascii=False))\nelse:\n    expected=json.loads((logs/'token_reference.json').read_text())\n    assert len(expected)==len(answers)\n    for i,(a,b) in enumerate(zip(expected,answers)):\n        assert a==b,(i,queries[i],a,b)\nprint('FTS_WORD_TOKEN_SEMANTICS_PASS',label,len(answers),flush=True)\nclient=root/'deps/3rd/u01/obclient/bin/obclient';env=os.environ.copy();env['LD_LIBRARY_PATH']=str(client.parent.parent/'lib')\nenv.update(MYSQL=str(client)+' -h127.0.0.1 -P12889 -uroot -N -s -A --default-character-set=utf8mb4',MYSQL_VERBOSE=str(client)+' -h127.0.0.1 -P12889 -uroot -A --default-character-set=utf8mb4',LABEL=label,ROWS='20000',BATCH='500',ROUNDS='3000',QUERY_ROUNDS='200',SAMPLES='3',WARMUP='30',SKIP_LOAD='0',OUTPUT=str(logs/(label+'_report.txt')))\nwith (logs/(label+'.log')).open('wb') as out:\n    result=subprocess.run(['bash','tools/benchmark/fts_large_bench.sh'],cwd=root,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=900)\nassert result.returncode==0,(label,result.returncode)\nprint('FTS_WORD_COMPLETE_BENCH_PASS',label,flush=True)\nprint((logs/(label+'_report.txt')).read_text(),flush=True)\n"
(logs/'verify.py').write_text(verify)
start(b/'fts_count_hotloop/seekdb_candidate')
with (logs/'before_validation.log').open('wb') as out:r=subprocess.run([str(b/'venv/bin/python'),str(logs/'verify.py'),'before'],stdout=out,stderr=out)
assert r.returncode==0,'Baseline failed'
print('COMPACT_DAT_BASELINE_PASS',flush=True)
source=Path('/mnt/f/\u7814\u7a76\u751f/\u6570\u636e\u5e93/completion_20261116/work/fts_small_union_candidate')
for rel in ['src/observer/composition/retrieval/ob_das_legacy_tr_merge_iter.cpp', 'src/sql/code_generator/ob_tsc_cg_service.cpp', 'src/sql/das/ob_das_ir_define.h', 'src/storage/retrieval/ob_text_daat_iter.cpp', 'src/storage/retrieval/ob_text_daat_iter.h']:
 target=root/rel;backup=logs/'before_source'/rel;backup.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(target,backup)
 if target.read_bytes()!=(source/rel).read_bytes():shutil.copyfile(source/rel,target)
env=dict(os.environ,RUSTUP_HOME=str(b/'rustup'),CARGO_HOME=str(b/'cargo'));env['PATH']=str(b/'cargo/bin')+':'+str(root/'deps/3rd/usr/local/oceanbase/devtools/bin')+':'+env['PATH']
with (logs/'build.log').open('wb') as out:r=subprocess.run(['make','-C',str(root/'build_release'),'-j2','seekdb'],env=env,stdout=out,stderr=out)
assert r.returncode==0,'Build failed'
shutil.copyfile(root/'build_release/src/observer/seekdb',logs/'seekdb_candidate');(logs/'seekdb_candidate').chmod(0o755)
print('COMPACT_DAT_BUILD_PASS',flush=True);start(logs/'seekdb_candidate')
with (logs/'after_validation.log').open('wb') as out:r=subprocess.run([str(b/'venv/bin/python'),str(logs/'verify.py'),'after'],stdout=out,stderr=out)
assert r.returncode==0,'Candidate validation failed'
print('COMPACT_DAT_SEMANTICS_AND_COMPLETE_BENCH_PASS',flush=True)
(logs/'result.json').write_text(json.dumps({'status':'PASS','baseline_binary':str(b/'fts_count_hotloop/seekdb_candidate'),'candidate_binary':str(logs/'seekdb_candidate'),'same_official_benchmark':True}))
