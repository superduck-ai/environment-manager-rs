"""Independent exact JSON checks; no response normalization or fixture reuse."""
import hashlib,itertools,json,os,pathlib,re,shutil,subprocess,sys
from probe import ROOT,call
OUT=ROOT/'recovery/evidence';BASE=ROOT/'recovery/adversarial-alignment'
BINARY=sys.argv[1] if len(sys.argv)>1 else os.environ.get('ALIGNMENT_BINARY','recovery/rebuilt-mcp-server')
LITERALS=['pid: 314159','/tmp/tool-outputs/arbitrary-user-path.txt','/tmp/bash-user-path.sh','pid: 271828 /tmp/tool-outputs/nested/path', '/tmp/tool-outputs/bash_123456.log', '/tmp/tool-outputs/grep_1736000000000000000.txt', '2026/10/09 12:34:56 user text']
requests=[]
for i,s in enumerate(LITERALS):
 requests += [call('read',{'file_path':f'/data/literal-{i}'},len(requests)+1),call('write',{'file_path':f'/data/write-{i}','content':s},len(requests)+2),call('edit',{'file_path':f'/data/write-{i}','old_string':s,'new_string':s+' changed'},len(requests)+3),call('bash',{'command':"printf '%s' '"+s+"'"},len(requests)+4),call('grep',{'path':f'literal-{i}','pattern':'.','output_mode':'content'},len(requests)+5)]
patterns=['literal','-literal','--literal','-','é','{a,b}',' x ', '\x00']
for pattern in patterns:
 for extra in [{},{'include':'a,b {c,d}.go e\u2003f','type':'go','multiline':'true','-i':.5,'-n':'1','context':2}]:
  requests.append(call('grep',{'path':'argv-target','pattern':pattern,'output_mode':'content',**extra},len(requests)+1))
results=[]
for label,binary in [('original','mcp-server'),('rebuilt',BINARY)]:
 p=BASE/label;shutil.rmtree(p,ignore_errors=True);p.mkdir(parents=True)
 for i,s in enumerate(LITERALS):(p/f'literal-{i}').write_text(s+'\n')
 (p/'argv-target').write_text('irrelevant\n');(p/'rg-bin').mkdir()
 (p/'rg-bin/rg').write_text('#!/usr/bin/python3\nimport json,sys\nargs=sys.argv[1:]\nif args[-1]=="/data/argv-target": print(json.dumps({"type":"match","data":{"path":{"text":"/data/argv-target"},"lines":{"text":json.dumps(args)},"line_number":1}}))\nelse:\n import os\n os.execv("/usr/bin/rg",["rg"]+args)\n')
 (p/'rg-bin/rg').chmod(0o755)
 cmd=['docker','run','--rm','-i','--platform','linux/amd64','--network','none','-v',str(ROOT)+':/src:ro','-v',str(p)+':/data','mcp-recovery-oracle','sh','-c',f'cp /src/{binary} /tmp/server;chmod +x /tmp/server;PATH=/data/rg-bin:$PATH /tmp/server -workdir /data']
 proc=subprocess.run(cmd,input=''.join(json.dumps(r)+'\n' for r in requests),capture_output=True,text=True,timeout=60)
 response=[json.loads(l) for l in proc.stdout.splitlines()]
 snapshot={str(f.relative_to(p)):hashlib.sha256(f.read_bytes()).hexdigest() for f in p.rglob('*') if f.is_file()}
 results.append({'responses':response,'snapshot':snapshot,'exit_code':proc.returncode,'stderr':proc.stderr})
fail=[]
for i,(a,b) in enumerate(itertools.zip_longest(results[0]['responses'],results[1]['responses'])):
 if json.dumps(a,sort_keys=True)!=json.dumps(b,sort_keys=True):fail.append({'case':i+1,'request':requests[i],'original':a,'rebuilt':b})
for r in results:
 if len(r['responses'])!=len(requests) or r['exit_code']:fail.append({'count':len(r['responses']),'expected':len(requests),'exit_code':r['exit_code']})
if results[0]['snapshot']!=results[1]['snapshot']:fail.append({'snapshot':results})
# Fail if the controlled executable was bypassed or all probes silently became "No matches".
argv=[]
for i,resp in enumerate(results[0]['responses'][len(LITERALS)*5:],len(LITERALS)*5):
 text=resp.get('result',{}).get('content',[{}])[0].get('text','')
 if '\x00' in requests[i]['params']['arguments']['pattern']:
  if not resp.get('result',{}).get('isError') or 'invalid argument' not in text: fail.append({'nul_argument_error':resp})
  continue
 try:
  args=json.loads(text.split(':1:',1)[1]);argv.append(args)
 except (ValueError,IndexError):fail.append({'oracle_wrapper_not_observed':requests[i],'response':resp});continue
 pat=requests[i]['params']['arguments']['pattern']
 if pat.startswith('-') and ('-e' not in args or args[args.index('-e')+1]!=pat):fail.append({'conditional_dash_pattern':args,'pattern':pat})
 if not pat.startswith('-') and '-e' in args:fail.append({'unconditional_dash_e':args,'pattern':pat})
regressions=[]
# Keep special mode bits on the Linux filesystem, not a macOS bind mount.
for name,user,setup,rs in [
 ('special-mode-and-glob','0:0','printf old > /tmp/mode; chmod 7755 /tmp/mode',[
  call('write',{'file_path':'/tmp/mode','content':'new'},1),
  call('bash',{'command':"stat -c '%a' /tmp/mode"},2),
  call('glob',{'pattern':'{a,b}['},3)]),
 ('symlink-errors','65534:65534','mkdir /tmp/locked; printf old > /tmp/locked/file; chmod 555 /tmp/locked; ln -s /tmp/locked/file /tmp/link; ln -s /tmp/locked /tmp/dir-link',[
  call('write',{'file_path':'/tmp/link','content':'new'},1),
  call('write',{'file_path':'/tmp/dir-link/.','content':'new'},2)]),
]:
 pair=[]
 for binary in ['mcp-server',BINARY]:
  cmd=['docker','run','--rm','-i','--platform','linux/amd64','--network','none','--user',user,'-v',str(ROOT)+':/src:ro','mcp-recovery-oracle','sh','-c',f'set -eu; cp /src/{binary} /tmp/server; chmod +x /tmp/server; {setup}; /tmp/server -workdir /tmp']
  proc=subprocess.run(cmd,input=''.join(json.dumps(r)+'\n' for r in rs),capture_output=True,text=True,timeout=60)
  responses=[json.loads(line) for line in proc.stdout.splitlines()]
  pair.append(responses)
  if proc.returncode or len(responses)!=len(rs):fail.append({'regression':name,'binary':binary,'exit_code':proc.returncode,'responses':responses,'stderr':proc.stderr})
 for i,(a,b) in enumerate(itertools.zip_longest(*pair)):
  if json.dumps(a,sort_keys=True)!=json.dumps(b,sort_keys=True):fail.append({'regression':name,'request':rs[i],'original':a,'rebuilt':b})
 regressions.append({'name':name,'requests':rs,'original':pair[0],'rebuilt':pair[1]})
report={'cases':len(requests),'responses':[len(x['responses']) for x in results],'failures':fail,'normalization':'none; strict serialized parsed JSON, file hashes, response counts and process exit status','original_sha256':hashlib.sha256((ROOT/'mcp-server').read_bytes()).hexdigest(),'rebuilt_sha256':hashlib.sha256((ROOT/BINARY).read_bytes()).hexdigest(),'controlled_original_argv':argv,'original_responses':results[0]['responses']}
report['regressions']=regressions
report['cases']+=sum(len(r['requests']) for r in regressions)
report['responses']=[report['responses'][i]+sum(len(r[key]) for r in regressions) for i,key in enumerate(['original','rebuilt'])]
(OUT/'adversarial-literals-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print(json.dumps({'cases':report['cases'],'failures':len(fail),'responses':report['responses']}))
for f in fail[:10]:print(json.dumps(f,ensure_ascii=False))
sys.exit(bool(fail))
