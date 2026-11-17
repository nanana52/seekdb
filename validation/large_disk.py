"""Exercise a database larger than its hard container memory budget."""
from pathlib import Path
import argparse,json,os,select,subprocess,time
class Container:
 def __init__(self,name,data,image):self.name=name;self.data=data;self.image=image;self.i=0;self.start()
 def line(self,timeout=120):
  r,_,_=select.select([self.p.stdout],[],[],timeout)
  if not r:raise TimeoutError('Container response timeout')
  raw=self.p.stdout.readline()
  if not raw:raise RuntimeError(self.p.stderr.read().decode()[-1200:])
  return json.loads(raw)
 def start(self):
  self.p=subprocess.Popen(['docker','run','--name',self.name,'-i','--network=none','--cpus=2','--memory=2g','--memory-swap=2g','--pids-limit=128','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','-v',str(self.data)+':/data',self.image],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,bufsize=0)
  assert self.line()=={'type':'ready'}
 def sql(self,s):
  self.i+=1;self.p.stdin.write((json.dumps({'id':self.i,'op':'execute','sql':s,'search_mode':'exact'})+'\n').encode());r=self.line();assert r['id']==self.i and r['ok'],r;return r
 def stop(self,kill=False):
  if kill:subprocess.run(['docker','kill',self.name],check=True,stdout=subprocess.DEVNULL)
  else:
   self.i+=1;self.p.stdin.write((json.dumps({'id':self.i,'op':'shutdown'})+'\n').encode());assert self.line()=={'id':self.i,'ok':True}
  self.p.wait(timeout=30)
  inspect=json.loads(subprocess.check_output(['docker','inspect',self.name]))[0]
  subprocess.run(['docker','rm',self.name],check=True,stdout=subprocess.DEVNULL)
  assert not inspect['State']['OOMKilled'],inspect['State']
  return inspect['State']
def main(a):
 root=a.output;root.mkdir(parents=True,exist_ok=False);data=root/'data';data.mkdir();db=Container('minivectordb-large-20261117',data,a.image)
 n=64000;width=35000;payload='d'*width
 db.sql('create table large_docs(id bigint primary key,payload varchar(35000),value bigint)')
 begin=time.perf_counter()
 for at in range(0,n,100):
  sql='insert into large_docs values '+','.join('('+str(i)+",'"+payload+"',"+str(i*3)+')' for i in range(at,at+100))
  assert db.sql(sql)['affected_rows']==100
  if at%5000==0:print('LARGE_DISK_INSERT',at+100,n,flush=True)
 write_sec=time.perf_counter()-begin;bytes_before=(data/'data.wal').stat().st_size;assert bytes_before>2*1024**3
 db.sql('begin');assert db.sql("update large_docs set payload='pending' where id < 100")['affected_rows']==100
 first_state=db.stop(kill=True);begin=time.perf_counter();db.start();recovery_sec=time.perf_counter()-begin
 rows=db.sql('select id,value from large_docs order by id')['rows'];assert rows==[[i,i*3] for i in range(n)]
 for i in [0,77,100,32000,n-1]:assert db.sql('select payload from large_docs where id='+str(i))['rows']==[[payload]]
 db.sql('begin');assert db.sql('delete from large_docs where id >= 1000')['affected_rows']==63000;db.sql('rollback');assert len(db.sql('select id from large_docs')['rows'])==n
 second_state=db.stop();report={'status':'PASS','scope':'Actual network-none readonly-root Docker2CPU2GiB/no-swap128PID; >memory database with real SIGKILL and restart','rows':n,'payload_chars':width,'committed_wal_bytes':bytes_before,'container_memory_limit_bytes':2*1024**3,'data_exceeds_memory':True,'write_sec':write_sec,'recovery_sec':recovery_sec,'committed_row_count_verified':n,'uncommitted_update_rolled_back':True,'large_delete_rollback':True,'first_container_OOM':first_state['OOMKilled'],'second_container_OOM':second_state['OOMKilled']}
 (root/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--image',default='minivectordb:20261117');p.add_argument('--output',type=Path,required=True);main(p.parse_args())
