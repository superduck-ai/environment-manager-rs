import json,subprocess,pathlib,sys,itertools
from probe import ROOT
from differential import canonical,normalize,fixture
fixture(ROOT/'recovery/transports-data')
results=[]
for binary in ['mcp-server','recovery/rebuilt-mcp-server']:
 p=subprocess.run(['docker','run','--rm','--platform','linux/amd64','--network','none','-v',str(ROOT)+':/src:ro','-v',str(ROOT/'recovery/transports-data')+':/data','mcp-recovery-oracle','sh','-c',f'cp /src/{binary} /tmp/server; chmod +x /tmp/server; python3 /src/recovery/tools/transports_inside.py /tmp/server'],capture_output=True,text=True,timeout=180)
 if p.returncode:print(p.stderr);sys.exit(p.returncode)
 results.append(json.loads(p.stdout))
fail=[{'original':a,'rebuilt':b} for a,b in itertools.zip_longest(*results) if canonical(normalize(a))!=canonical(normalize(b))]
if any(len(r)!=24 for r in results):fail.append({'response_counts':[len(r) for r in results],'expected':24})
report={'cases':len(results[0]),'failures':fail,'original':results[0],'rebuilt':results[1]};(ROOT/'recovery/evidence/transports-report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False));print('Cases',len(results[0]),'failures',len(fail))
for f in fail:print(json.dumps(f,ensure_ascii=False)[:5000])
sys.exit(bool(fail))
