from pathlib import Path
import json,os,signal,socket,subprocess,time
BASE=Path('/opt/oceanbase_completion_20261116')
ROOT=BASE/'seekdb'
OUT=Path(__file__).resolve().parent/'test_logs'
OUT.mkdir(exist_ok=True)
# Wait for the original independent build, then build all edits made during it.
while Path('/proc/632426').exists():
    time.sleep(2)
with (OUT/'ai_native_vector_fix_build.log').open('wb') as log:
    env=os.environ.copy()
    env['RUSTUP_HOME']=str(BASE/'rustup');env['CARGO_HOME']=str(BASE/'cargo')
    env['PATH']=str(BASE/'cargo/bin')+':'+str(ROOT/'deps/3rd/usr/local/oceanbase/devtools/bin')+':'+env['PATH']
    build=subprocess.run(['make','-C',str(ROOT/'build_release'),'-j2','seekdb'],env=env,stdout=log,stderr=subprocess.STDOUT)
if build.returncode:
    print('AI_NATIVE_FIX_BUILD_FAILED',build.returncode,flush=True)
    raise SystemExit(build.returncode)
print('AI_NATIVE_FIX_BUILD_PASS',flush=True)
runtime=BASE/'ai_column_strict_retry_20261117'
runtime.mkdir(exist_ok=True)
# Stop only this isolated instance; the course database is not involved.
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():continue
    try:args=(proc/'cmdline').read_bytes().split(b'\0')
    except OSError:continue
    if args and args[0].endswith(b'/seekdb') and ('--base-dir='+str(runtime)).encode() in args:
        os.kill(int(proc.name),signal.SIGTERM)
        for _ in range(60):
            if not proc.exists():break
            time.sleep(0.5)
        if proc.exists():raise RuntimeError('Isolated instance did not stop; preserving process')
log=(runtime/'stdout.log').open('ab')
server=subprocess.Popen([str(ROOT/'build_release/src/observer/seekdb'),'--base-dir='+str(runtime),'--port=12889','--nodaemon','--parameter','memory_limit=2G','--parameter','log_disk_size=2G'],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
(runtime/'pid').write_text(str(server.pid))
for _ in range(90):
    if server.poll() is not None:raise RuntimeError('Candidate exited: '+str(server.returncode))
    try:
        with socket.create_connection(('127.0.0.1',12889),timeout=1):break
    except OSError:time.sleep(1)
else:raise RuntimeError('Candidate did not become ready')
env=os.environ.copy();env['LD_LIBRARY_PATH']=str(ROOT/'deps/3rd/u01/obclient/lib')
results=[]
for name in ['ai_column_ddl','ai_column_t1_dml','ai_column_worker','ai_column_vector_index','ai_column_retry_strict']:
    suite=ROOT/'tools/deploy/mysql_test/test_suite/ai_column'
    cmd=[str(ROOT/'deps/3rd/u01/obclient/bin/mysqltest'),'--host=127.0.0.1','--port=12889','--user=root','--database=test','--default-character-set=utf8mb4','--test-file='+str(suite/'t'/(name+'.test')),'--result-file='+str(suite/'r/mysql'/(name+'.result'))]
    start=time.monotonic()
    try:
        res=subprocess.run(cmd,cwd=ROOT/'tools/deploy',env=env,capture_output=True,timeout=300)
        code=res.returncode;output=res.stdout+res.stderr
    except subprocess.TimeoutExpired as error:
        code=124;output=(error.stdout or b'')+(error.stderr or b'')+b'\nTIMEOUT\n'
    (OUT/('ai_native_final_'+name+'.log')).write_bytes(output)
    record={'test':name,'exit':code,'seconds':round(time.monotonic()-start,2)}
    results.append(record)
    (OUT/'ai_native_final_results.json').write_text(json.dumps(results,indent=2))
    print(json.dumps(record),output.decode(errors='replace')[-3000:],flush=True)
    if code:
        # Preserve the first failure state for diagnosis before another test changes it.
        break
print('AI_NATIVE_FINAL_SUITE_PASS' if len(results)==5 and all(x['exit']==0 for x in results) else 'AI_NATIVE_FINAL_SUITE_NEEDS_REPAIR',flush=True)
