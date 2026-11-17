
from pathlib import Path
import subprocess,os,shutil,time,signal,sys,json
base=Path('/opt/oceanbase_completion_20261116');root=base/'seekdb';logs=base/'fts_count_only';candidate=logs/'seekdb_candidate';runtime=base/'ai_column_strict_retry_20261117'
with (root/'build_release/src/observer/seekdb').open('rb') as s,candidate.open('wb') as out:shutil.copyfileobj(s,out,1024*1024)
candidate.chmod(0o755)
for n in ['src/sql/optimizer/ob_log_plan.cpp','src/sql/rewrite/ob_transform_pre_process.cpp']:shutil.copyfile(Path('/mnt/f/研究生/数据库/completion_20261116/work/seekdb')/n,root/n)
env=os.environ.copy();env['RUSTUP_HOME']=str(base/'rustup');env['CARGO_HOME']=str(base/'cargo');env['PATH']=str(base/'cargo/bin')+':'+str(root/'deps/3rd/usr/local/oceanbase/devtools/bin')+':'+env['PATH']
with (logs/'baseline_build.log').open('wb') as out:r=subprocess.run(['make','-C',str(root/'build_release'),'-j2','seekdb'],env=env,stdout=out,stderr=out)
assert r.returncode==0
print('FTS_BASELINE_REBUILT',flush=True)
def start(binary):
 p=Path('/proc')/(runtime/'pid').read_text().strip()
 if p.exists() and (p/'stat').read_text().split()[2]!='Z':
  assert ('--base-dir='+str(runtime)).encode() in (p/'cmdline').read_bytes();os.kill(int(p.name),signal.SIGTERM)
  for _ in range(100):
   if not p.exists() or (p/'stat').read_text().split()[2]=='Z':break
   time.sleep(.2)
  else:raise RuntimeError('Instance did not stop')
 with (runtime/'stdout.log').open('ab') as out:p=subprocess.Popen([str(binary),'--base-dir='+str(runtime),'--port=12889','--nodaemon','--parameter','memory_limit=2G','--parameter','log_disk_size=2G'],stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
 (runtime/'pid').write_text(str(p.pid));time.sleep(8);assert p.poll() is None
for label,binary in [('before',root/'build_release/src/observer/seekdb'),('after',candidate)]:
 start(binary)
 client=root/'deps/3rd/u01/obclient/bin/obclient';bench_env=env.copy();bench_env['LD_LIBRARY_PATH']=str(client.parent.parent/'lib')
 bench_env.update(MYSQL=str(client)+' -h127.0.0.1 -P12889 -uroot -N -s -A --default-character-set=utf8mb4',MYSQL_VERBOSE=str(client)+' -h127.0.0.1 -P12889 -uroot -A --default-character-set=utf8mb4',LABEL=label,ROWS='20000',BATCH='500',ROUNDS='3000',QUERY_ROUNDS='200',SAMPLES='3',WARMUP='30',SKIP_LOAD='0',OUTPUT=str(logs/(label+'_report.txt')))
 with (logs/(label+'_benchmark.log')).open('wb') as out:r=subprocess.run(['bash','tools/benchmark/fts_large_bench.sh'],cwd=root,env=bench_env,stdout=out,stderr=out,timeout=900)
 assert r.returncode==0,label
 print('FTS_PAIRED_COMPLETE_BENCH',label,(logs/(label+'_report.txt')).read_text(),flush=True)
print('FTS_COUNT_ONLY_PAIRED_BENCH_FINISHED',flush=True)
