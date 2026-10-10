"""Original-versus-reconstruction Bash/interactive differential; isolated Linux fixtures."""
import hashlib,itertools,json,os,pathlib,random,re,shutil,subprocess,sys
from probe import ROOT,call
from differential import canonical,normalize,snapshot
BASE=ROOT/'recovery/process-alignment'; EVIDENCE=ROOT/'recovery/evidence'

def cases():
 out=[]
 def add(category,args):out.append({'category':category,'request':call('bash',args,len(out)+1)})
 # Invalid single-key env makes every accepted matcher probe harmless. Detection precedes env parsing.
 names=['vim','vi','nvim','nano','emacs','pico','joe','less','more','man','python','python2','python3','node','irb','mysql','psql','sqlite3','cat','tail -f file','tail -F file','top','htop','btop','atop','glances','watch x','git add -p','git add --patch','git rebase -i','git rebase --interactive','true','echo ok']
 wrappers=[lambda x:x,lambda x:' sudo '+x,lambda x:x.upper(),lambda x:'echo ok; '+x,lambda x:'true && '+x,lambda x:'echo ok | '+x,lambda x:x+' | true',lambda x:'"'+x+'"',lambda x:"echo '"+x+"'",lambda x:'echo "'+x+'"',lambda x:'; ; '+x+';',lambda x:x+'\n',lambda x:'\u2003'+x+'\u00a0',lambda x:'echo \\"; '+x]
 for n in names:
  for w in wrappers:add('interactive-rules',{'command':w(n),'env':{'-':'invalid'}})
 rng=random.Random(772103)
 atoms=names+[';', '&&','|','||','&',"'",'"','\\', '\n','\t','中','echo', ' ', '$(',')']
 for _ in range(900):add('interactive-fuzz',{'command':''.join(rng.choice(atoms) for _ in range(rng.randrange(1,8))),'env':{'-':'invalid'}})
 for v in [None,False,True,[],['x'],'x',0,3,{}, {'':'x'},{'0A':'x'},{'a=b':'x'},{'A-B':'x'},{'中文':'x'},{'A\n':'x'},{'A':None},{'A':4},{'A':[]},{'A':{}},{'A':'\0'},{'A':'x\ny=z'},{'_A':'ok'}]:add('env-validation',{'command':'printf "%s" "$A$_A"','env':v})
 for v in [None,False,True,'1',{},[],0,-1,0.1,0.9,1,1.9,2,600,601,1e20]:add('timeout-argument',{'command':'printf timeout-arg','timeout':v})
 for v in [None,False,True,'','KILL','kill','background',1,{},[]]:add('timeout-mode',{'command':'true','on_timeout':v})
 for v in [None,False,True,0,'',' ','nested','./nested','nested/..','/data/nested','missing','/data/lines.txt','\0']:add('work-dir',{'command':'pwd','work_dir':v})
 for c in ['true','printf hi','echo hi','echo hi; exit 7','exit 1','exit 255','exit -1','printf x >&2; exit 3','kill -TERM $$','kill -KILL $$','kill -INT $$','printf "a\\r\\nb\\000c"','printf "\\200\\201x"','false | true','set -o pipefail; false | true'] :add('completion-and-signal',{'command':c})
 add('session-and-process-group',{'command':'''python3 -c 'import os,json;p=os.getppid();print(json.dumps({"session_leader":os.getsid(p)==p,"group_leader":os.getpgid(p)==p},sort_keys=True))'; true'''})
 for count in [29982,29983,29984,29985,29986,29987,30000,30001,30100]:
  add('output-byte-boundary',{'command':"python3 -c 'import sys;sys.stdout.write(\"x\"*%d)'"%count})
 for count in [9994,9995,9996,9997,10000,10001]:add('utf8-tail',{'command':"python3 -c 'import sys;sys.stdout.write(\"你\"*%d)'"%count})
 for v in [0.9,1.9,'1',True]:add('observed-timeout-conversion',{'command':'sleep 1.6; printf converted','timeout':v})
 add('timeout-term',{'command':'printf started; sleep 8','timeout':1})
 add('timeout-grace',{'command':'trap "" TERM; printf ignore-term; sleep 8','timeout':1})
 add('descendant-kill',{'command':"bash -c 'trap \"\" TERM; sleep 5; printf late > /data/descendant-late' & wait",'timeout':1})
 add('descendant-kill-observe',{'command':'sleep 1; if test -e /data/descendant-late; then echo survived; else echo absent; fi'})
 add('background-large',{'command':"python3 -c 'print(\"x\"*5000,end=\"\")'; sleep 2",'timeout':1,'on_timeout':'background'})
 add('background-large-settle',{'command':'sleep 2; printf settled'})
 add('background',{'command':'sleep 2; printf background-done > /data/background-done; printf done','timeout':1,'on_timeout':'background'})
 add('background-observe',{'command':'sleep 2; cat /data/background-done; printf "\\n"; python3 /data/artifacts.py'})
 add('vault-source',{'command':'printf "%s" "$VAULT_VALUE"','env':{'QODER_VAULT_ENV_FILE':'/data/vault.sh'}})
 add('vault-overrides-call-env',{'command':'printf "%s" "$VAULT_VALUE"','env':{'QODER_VAULT_ENV_FILE':'/data/vault.sh','VAULT_VALUE':'call'}})
 add('output-unlinked',{'command':'rm /tmp/tool-outputs/bash_*.log; printf deleted'})
 add('background-preview-failure',{'command':'rm /tmp/tool-outputs/bash_*.log; sleep 2','timeout':1,'on_timeout':'background'})
 add('after-background-preview-failure',{'command':'sleep 2; printf settled'})
 add('artifact-cleanup',{'command':'python3 /data/artifacts.py'})
 return out

def logs(s):
 s=normalize(s)
 s=re.sub(r'^\d{4}/\d\d/\d\d \d\d:\d\d:\d\d ', '<time> ',s,flags=re.M)
 s=re.sub(r'\b(pid|pgid)=\d+',r'\1=<pid>',s)
 return s

def fixture(p):
 shutil.rmtree(p,ignore_errors=True);p.mkdir(parents=True);(p/'nested').mkdir();(p/'lines.txt').write_text('x\n');(p/'vault.sh').write_text('export VAULT_VALUE=from-vault\n')
 (p/'artifacts.py').write_text('import pathlib,json,hashlib\na=[]\nfor p in pathlib.Path("/tmp/tool-outputs").glob("*"):\n if p.suffix==".sh": continue\n b=p.read_bytes()\n if b:a.append([len(b),hashlib.sha256(b).hexdigest()])\nprint(json.dumps(sorted(a)))\n')

def run(rs,binary,path,setup=''):
 cmd=['docker','run','--rm','-i','--platform','linux/amd64','--network','none','-v',str(ROOT)+':/src:ro','-v',str(path)+':/data','mcp-recovery-oracle','sh','-c',f'cp /src/{binary} /tmp/server; chmod +x /tmp/server; {setup} /tmp/server -workdir /data']
 p=subprocess.run(cmd,input=''.join(json.dumps(r)+'\n' for r in rs),capture_output=True,text=True,timeout=180)
 return [json.loads(x) for x in p.stdout.splitlines()],p.stderr,p.returncode

def main():
 binary=sys.argv[1] if len(sys.argv)>1 else os.environ.get('ALIGNMENT_BINARY','recovery/process-rebuilt'); cs=cases(); failures=[];verified=[];BASE.mkdir(exist_ok=True)
 for group, subset, setup in [('full',cs,''),('output-dir-startup',[{'category':'output-dir-startup','request':call('bash',{'command':'true'},1)}],'touch /tmp/tool-outputs;')]:
  outputs=[]
  for label,binpath in [('original','mcp-server'),('rebuilt',binary)]:
   p=BASE/(group+'-'+label);fixture(p);outputs.append((run([c['request'] for c in subset],binpath,p,setup),snapshot(p)))
  (a,sa),(b,sb)=outputs
  if a[2]!=b[2] or a[2]:failures.append({'group':group,'exit_codes':[a[2],b[2]]})
  if len(a[0])!=len(subset) or len(b[0])!=len(subset):failures.append({'group':group,'response_counts':[len(a[0]),len(b[0])]})
  for i,(x,y) in enumerate(itertools.zip_longest(a[0],b[0])):
   c=subset[i] if i<len(subset) else {'category':'unexpected'}
   if canonical(normalize(x))!=canonical(normalize(y)):failures.append(dict(c,original=x,rebuilt=y))
   else:verified.append({'group':group,'category':c['category'],'id':c.get('request',{}).get('id'),'response_sha256':hashlib.sha256(json.dumps(normalize(x),ensure_ascii=False,sort_keys=True).encode()).hexdigest()})
  if logs(a[1])!=logs(b[1]):failures.append({'group':group,'stderr_original':logs(a[1]),'stderr_rebuilt':logs(b[1])})
  if sa!=sb:failures.append({'group':group,'snapshot_original':sa,'snapshot_rebuilt':sb})
  (EVIDENCE/f'process-{group}-original-stderr.txt').write_text(a[1]);(EVIDENCE/f'process-{group}-rebuilt-stderr.txt').write_text(b[1])
 timing=[]
 for label,binpath in [('original','mcp-server'),('rebuilt',binary)]:
  p=BASE/('timing-'+label);fixture(p)
  cmd=['docker','run','--rm','--platform','linux/amd64','--network','none','-v',str(ROOT)+':/src:ro','-v',str(p)+':/data','mcp-recovery-oracle','sh','-c',f'cp /src/{binpath} /tmp/server; chmod +x /tmp/server; python3 /src/recovery/tools/process_timing_inside.py /tmp/server']
  proc=subprocess.run(cmd,capture_output=True,text=True,timeout=60)
  if proc.returncode: failures.append({'timing_execution':label,'stderr':proc.stderr}); continue
  data=json.loads(proc.stdout);timing.append(data)
  for item in data['timing']:
   if not item['within_bounds']:failures.append({'timing_bounds':label,'result':item})
  if not data['http_cancellation']['child_stopped'] or data['http_cancellation']['late_write']:failures.append({'http_cancellation':label,'result':data['http_cancellation']})
 if len(timing)==2:
  for x,y in zip(timing[0]['timing'],timing[1]['timing']):
   if canonical(normalize(x['response']))!=canonical(normalize(y['response'])):failures.append({'timing_response_original':x,'timing_response_rebuilt':y})
  for key in ['stdio_stderr','http_stderr']:
   if logs(timing[0][key])!=logs(timing[1][key]):failures.append({'timing_logs':key,'original':logs(timing[0][key]),'rebuilt':logs(timing[1][key])})
 report={'requests':len(cs)+5,'timing_observations':timing,'failures':failures,'verified':verified,'normalization':['temporary artifact paths','process IDs','stderr log timestamp'],'oracle':'mcp-server','rebuilt':binary,'fixture_root':str(BASE),'compared':['complete JSON responses','stderr lifecycle/error logs','persistent fixture file SHA256 and permissions','output artifact sizes and hashes through observer commands']}
 (EVIDENCE/'process-alignment-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print(json.dumps({'requests':report['requests'],'failures':len(failures)}))
 for f in failures[:10]:print(json.dumps(f,ensure_ascii=False)[:3000])
 return bool(failures)
if __name__=='__main__':sys.exit(main())
