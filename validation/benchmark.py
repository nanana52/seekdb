from pathlib import Path
import argparse,json,math,os,random,struct,time
from oracle_recovery import DB,lit,row_sql,f32
import numpy as np

def run(binary,output):
 rng=np.random.default_rng(719552);n,dim,nq=10000,128,1000
 vectors=rng.normal(size=(n,dim)).astype('float32');queries=rng.normal(size=(nq,dim)).astype('float32');cats=np.arange(n)%8
 db=DB(binary,output/'database');db.sql('create table docs(id bigint primary key,category varchar(12),value bigint,embedding vector(128))')
 start=time.perf_counter()
 for at in range(0,n,100):
  sql='insert into docs values '+','.join(row_sql([i+100003,'cat'+str(cats[i]),i,vectors[i].tolist()]) for i in range(at,at+100))
  assert db.sql(sql)['affected_rows']==100
 write_sec=time.perf_counter()-start
 db.sql('create vector index docs_vector on docs(embedding)')
 # First query includes index construction; measured block below is after prewarming.
 db.sql('select id,L2_DISTANCE(embedding,'+lit(json.dumps(queries[0].tolist()))+') as d from docs order by d limit 10')
 text=[lit(json.dumps(q.tolist())) for q in queries]
 got=[];start=time.perf_counter()
 for i,q in enumerate(text):
  cond=" where category='cat"+str(i%8)+"'" if i%2 else ''
  got.append(db.sql('select id,L2_DISTANCE(embedding,'+q+') as d from docs'+cond+' order by d limit 10')['rows'])
 query_sec=time.perf_counter()-start
 # Independent double-precision oracle, filtering before the candidate sort.
 truth=[];distances=[];b=vectors.astype('float64')
 for at in range(0,nq,32):
  q=queries[at:at+32].astype('float64');sq=np.sum(q*q,axis=1)[:,None]+np.sum(b*b,axis=1)[None,:]-2*q@b.T
  for j in range(len(q)):
   i=at+j;eligible=np.nonzero(cats==i%8)[0] if i%2 else np.arange(n);idx=eligible[np.argpartition(sq[j,eligible],9)[:10]];idx=sorted(idx,key=lambda k:(sq[j,k],k))
   expected=[int(k+100003) for k in idx];truth.append(expected)
   rows=got[i];assert len(rows)==10 and len({r[0] for r in rows})==10
   for r in rows:
    k=r[0]-100003;assert 0<=k<n and (not i%2 or cats[k]==i%8)
    exact=math.sqrt(float(np.sum((b[k]-q[j])**2)));assert math.isclose(r[1],exact,rel_tol=1e-5,abs_tol=1e-5)
   assert [r[0] for r in rows]==expected,(i,[r[0] for r in rows],expected)
 with open('/proc/'+str(db.p.pid)+'/status') as f:hwm=next(int(x.split()[1])*1024 for x in f if x.startswith('VmHWM:'))
 db.restart(kill=True);assert len(db.sql('select id from docs order by id')['rows'])==n
 db.stop()
 report={'status':'PASS','scope':'Local native development benchmark; not formal cgroup score or rank','rows':n,'dimensions':dim,'queries':nq,'batch':100,'topk':10,'filtered_queries':500,'write_sec':write_sec,'query_sec':query_sec,'query_qps':nq/query_sec,'unfiltered_recall_at_10':1.0,'filtered_recall_at_10':1.0,'all_distances_match_current_float32_rows':True,'committed_data_survives_SIGKILL':True,'process_peak_rss_bytes':hwm,'data_bytes':sum(p.stat().st_size for p in (output/'database').iterdir() if p.is_file())}
 (output/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False);run(a.binary,a.output)
