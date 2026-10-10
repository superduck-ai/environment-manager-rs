"""Exact re-encoding checks for progressive JPEG, CMYK, sampling ratios and WebP."""
import hashlib,json,sys
from probe import ROOT,run,call
paths=sorted((ROOT/'recovery/image-formats-inputs').iterdir())
requests=[call('read',{'file_path':'/data/'+p.name},i+1) for i,p in enumerate(paths)]
results=[]
for binary in ['mcp-server','recovery/rebuilt-mcp-server']:
 rows,stderr,code=run(requests,binary,ROOT/'recovery/image-formats-inputs')
 if code or len(rows)!=len(requests):raise RuntimeError((binary,code,len(rows),stderr))
 results.append(rows)
failures=[];verified=[]
for path,a,b in zip(paths,*results):
 if a!=b:
  for row in [a,b]:
   for block in row.get('result',{}).get('content',[]):
    if 'data' in block:block['data']='sha256:'+hashlib.sha256(block['data'].encode()).hexdigest()
  failures.append({'file':path.name,'original':a,'rebuilt':b})
 else:verified.append({'file':path.name,'response_sha256':hashlib.sha256(json.dumps(a,sort_keys=True).encode()).hexdigest()})
report={'cases':len(requests),'failures':failures,'verified':verified}
(ROOT/'recovery/evidence/image-formats-report.json').write_text(json.dumps(report,indent=2))
print('Image formats',len(requests),'failures',len(failures))
for failure in failures:print(json.dumps(failure))
sys.exit(bool(failures))
