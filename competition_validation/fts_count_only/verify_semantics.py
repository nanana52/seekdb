
from pathlib import Path
import os,time,json,sys,pymysql,statistics
base=Path('/opt/oceanbase_completion_20261116');logs=base/'fts_count_only';logs.mkdir(exist_ok=True)
label=sys.argv[1]
con=pymysql.connect(host='127.0.0.1',port=12889,user='root',database='fts_large_bench',charset='utf8mb4',autocommit=True,read_timeout=60)
queries=[]
for mode in ['', ' in boolean mode']:
 for phrase in ['database audit','tokenizer','no_such_word_zzz']:
  match="match(content_en) against('"+phrase+"'"+mode+")" if phrase=='dummy' else "match(title_en,content_en) against('"+phrase+"'"+mode+")"
  for limit in [0,1,20,30000]:
   for offset in [0,5,30000]:
    sub='select id, category from docs where '+match+' limit '+str(limit)+' offset '+str(offset)
    queries.append('select count(*) from ('+sub+') t')
    if limit==20:
     queries.extend(['select count(id) from ('+sub+') t','select count(distinct category) from ('+sub+') t','select sum(id),count(*) from ('+sub+') t','select count(*) from ('+sub+') t where id%2=0'])
for condition in ['',' and category=2',' and id%3=1']:
 for limit in [1,20,1000]:
  sub="select id from docs where match(title_en,content_en) against('database audit')"+condition+' limit '+str(limit)
  queries.extend(['select count(*) from ('+sub+') t',sub])
queries.extend(["select count(*) from (select id from docs where match(content) against('tokenizer') order by id desc limit 20) t","select count(*) from (select id,match(title_en,content_en) against('database audit') as s from docs where match(title_en,content_en) against('database audit') order by s desc limit 20) t","select count(*) from (select id from docs where match(title_en,content_en) against('database audit') limit 20) t join docs d on t.id=d.id where d.id%2=0"])
answers=[]
with con.cursor() as c:
 for sql in queries:
  try:c.execute(sql);answers.append({'rows':c.fetchall()})
  except pymysql.Error as e:answers.append({'error':e.args[0]})
 normalized=json.loads(json.dumps(answers,default=str))
 if label=='before':(logs/'reference.json').write_text(json.dumps(normalized))
 else:
  ref=json.loads((logs/'reference.json').read_text());assert len(ref)==len(normalized)
  for i,(a,b) in enumerate(zip(ref,normalized)):assert a==b,(i,queries[i],a,b)
 print('FTS_COUNT_ONLY_RESULT_EQUIVALENCE_PASS',label,len(queries),flush=True)
 plans=[]
 for sql in ["select count(*) from (select id from docs where match(content) against('tokenizer') limit 20) t","select id from docs where match(content) against('tokenizer') limit 20"]:
  c.execute('explain '+sql);plans.append({'sql':sql,'plan':[x[0] for x in c.fetchall()]})
 (logs/(label+'_plans.json')).write_text(json.dumps(plans))
 q="select count(*) from (select id from docs where match(content) against('tokenizer') limit 20) t"
 samples=[]
 for _ in range(3):
  start=time.perf_counter()
  for _ in range(200):c.execute(q);c.fetchall()
  samples.append((time.perf_counter()-start)*1000/200)
 print('FTS_COUNT_LIMIT_LATENCY',label,json.dumps(samples),flush=True)
con.close()
