"""Independent SQL/state oracle and process-kill recovery checks."""
from pathlib import Path
import argparse,copy,json,math,os,random,select,struct,subprocess,tempfile,time
class DB:
 def __init__(self,binary,path):self.binary=binary;self.path=Path(path);self.path.mkdir(parents=True,exist_ok=True);self.i=0;self.start()
 def line(self,timeout=30):
  ready,_,_=select.select([self.p.stdout],[],[],timeout)
  if not ready:raise TimeoutError('Database response timeout')
  data=self.p.stdout.readline()
  if not data:raise RuntimeError('Database exited '+self.p.stderr.read().decode()[-1000:])
  return json.loads(data)
 def start(self):
  self.p=subprocess.Popen([str(self.binary),'--data-dir',str(self.path)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,bufsize=0);assert self.line()=={'type':'ready'}
 def sql(self,s,ok=True,mode='exact'):
  self.i+=1;self.p.stdin.write((json.dumps({'id':self.i,'op':'execute','sql':s,'search_mode':mode})+'\n').encode());r=self.line();assert r['id']==self.i and r['ok'] is ok,(s[:200],r);return r
 def stop(self,kill=False):
  if kill:self.p.kill();self.p.wait(timeout=10);return
  self.i+=1;self.p.stdin.write((json.dumps({'id':self.i,'op':'shutdown'})+'\n').encode());r=self.line();assert r=={'id':self.i,'ok':True};self.p.wait(timeout=10);assert self.p.returncode==0
 def restart(self,kill=False):self.stop(kill);self.start()
def f32(v):return struct.unpack('<f',struct.pack('<f',v))[0]
def lit(s):return "'"+s.replace('\\','\\\\').replace("'","''")+"'"
def row_sql(row):return '('+str(row[0])+','+lit(row[1])+','+str(row[2])+','+lit(json.dumps(row[3]))+')'
def run(binary,root):
 rng=random.Random(817311);db=DB(binary,root/'oracle');model={};checks=0
 db.sql('create table docs(id BIGINT NOT NULL PRIMARY KEY,category varchar(12),value BIGINT,embedding VECTOR(16));')
 for batch in range(8):
  rows=[]
  for i in range(batch*30,(batch+1)*30):
   row=[9007199254740993+i,'cat'+str(i%4),rng.randrange(-200,200),[f32(rng.uniform(-5,5)) for _ in range(16)]];model[row[0]]=row;rows.append(row)
  assert db.sql('insert into docs values '+','.join(map(row_sql,rows)))['affected_rows']==len(rows)
 db.sql('create vector index flat_docs on docs(embedding)')
 def verify():
  nonlocal checks
  rows=db.sql('select id,category,value,embedding from docs order by id')['rows'];assert len(rows)==len(model)
  for row,want in zip(rows,sorted(model.values())):
   assert row[:3]==want[:3];assert all(a==b for a,b in zip(row[3],want[3]))
  for j in range(25):
   q=[f32(rng.uniform(-5,5)) for _ in range(16)];cat='cat'+str(rng.randrange(5));k=rng.randrange(0,35);filtered=j%2;cond=' where (category='+lit(cat)+' AND value>=-150)' if filtered else ''
   candidates=[r for r in model.values() if not filtered or (r[1]==cat and r[2]>=-150)]
   expected=sorted([(sum((a-b)**2 for a,b in zip(r[3],q)),r[0]) for r in candidates])[:k]
   sql='select id,L2_DISTANCE(embedding,'+lit(json.dumps(q))+') AS d from docs'+cond+' order by d limit '+str(k)
   actual=db.sql(sql,mode='ann' if j%3==0 else 'exact')['rows'];assert len(actual)==len(expected)
   for got,(squared,key) in zip(actual,expected):assert got[0]==key and math.isclose(got[1],math.sqrt(squared),rel_tol=1e-5,abs_tol=1e-5),(got,key)
   checks+=1
 verify()
 for epoch in range(7):
  before=copy.deepcopy(model);db.sql('begin')
  for _ in range(18):
   key=rng.choice(list(model));row=copy.deepcopy(model[key]);row[2]=rng.randrange(-200,200);row[3]=[f32(rng.uniform(-5,5)) for _ in range(16)]
   db.sql('update docs set value='+str(row[2])+',embedding='+lit(json.dumps(row[3]))+' where id='+str(key));model[key]=row
  verify() # builds a flat index while uncommitted; later commit must relocate its offsets
  key=rng.choice(list(model));db.sql('insert into docs values '+row_sql([key,'dup',0,[0.0]*16]),ok=False)
  db.sql('insert into docs values '+row_sql([-(epoch+1),'ok',0,[0.0]*16])+','+row_sql([100+epoch,'bad',0,[0.0]]),ok=False)
  for invalid in ['[NaN,0]','[Infinity,0]','[3.5e39,0]']:
   db.sql('update docs set embedding='+lit(invalid)+' where id='+str(key),ok=False)
  verify()
  if epoch%2:db.sql('rollback');model=before
  else:db.sql('commit')
  verify();db.restart(kill=epoch%2==0);verify()
 # Unsubmitted transaction must disappear after a real SIGKILL.
 before=copy.deepcopy(model);db.sql('begin');db.sql('delete from docs where value >= -100');db.sql('create table pending_table(id bigint primary key)');db.restart(kill=True);model=before;verify();db.sql('select * from pending_table',ok=False)
 # Multiple tables and a newly created vector index roll back together.
 db.sql('begin');db.sql('create table temporary(id bigint primary key,v vector(2))');db.sql("insert into temporary values(1,'[1,2]')");db.sql('create vector index test_idx on temporary(v)');db.sql('rollback');db.sql('select * from temporary',ok=False)
 db.stop();wal=root/'oracle/data.wal';ack=wal.stat().st_size
 # Acknowledged commits survive arbitrary trailing incomplete frames.
 with wal.open('ab') as out:out.write(b'MVDBC003'+struct.pack('<QQ',8000,1234)+b'partial')
 db.start();verify();assert wal.stat().st_size==ack;db.stop()
 # Repeated interruption during actual recovery leaves committed source intact.
 for _ in range(6):
  p=subprocess.Popen([str(binary),'--data-dir',str(root/'oracle')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE);time.sleep(.001);p.kill();p.wait(timeout=10)
 db.start();verify();db.stop()
 report={'status':'PASS','scope':'independent deterministic Python state oracle and actual process-kill tests','seed':817311,'topk_comparisons':checks,'transaction_epochs':7,'committed_then_SIGKILL':True,'uncommitted_SIGKILL_rollback':True,'partial_tail_recovery_idempotent':True,'recovery_interrupted_repeatedly':6,'bigint_exact':True,'float32_exact_storage':True,'transaction_flat_index_relocation':True,'invalid_vector_statement_atomicity':True}
 (root/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--binary',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();a.output.mkdir(exist_ok=False,parents=True);run(a.binary,a.output)
