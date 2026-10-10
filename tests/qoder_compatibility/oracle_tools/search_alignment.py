"""Independent oracle checks of grep and glob; all fixtures scoped to search-alignment."""
import base64, hashlib, itertools, json, os, pathlib, random, re, shutil, subprocess, sys
from probe import ROOT, call
from differential import canonical,normalize
base=ROOT/'recovery/search-alignment';shutil.rmtree(base,ignore_errors=True);base.mkdir()
names=['a','b','abc','a01.txt','a02.txt','a03.txt','-01.txt','-00.txt','00.txt','01.txt','é.txt','中.txt','😀.txt','a:b.txt',' a.txt','a b.txt','a,b.txt','a{b}.txt','[a].txt','a!.txt','a).txt','a(.txt','1.txt','0.txt','9.txt','_.txt','.hidden.txt','a\\b.txt','a|b.txt']
for i,n in enumerate(names):
 for pre in ['', 'dir/', 'dir/sub/']:
  p=base/(pre+n);p.parent.mkdir(parents=True,exist_ok=True);p.write_text('START\nNeedle two Needle\nneedle\nEND\n',encoding='utf8');os.utime(p,(1700000000+i//2,1700000000+i//2))
for n,b in {'invalid.txt':b'Needle \xff\xfe\nend\n','crlf.txt':b'Needle\r\nnext\r\n','binary.txt':b'Needle\x00more\n','long.txt':b'Needle '+b'x'*501+b'\n','multiline.txt':b'Needle\nNeedle\n','empty.txt':b''}.items():
 (base/n).write_bytes(b)
for sub in ['.git','.svn','.hg','.bzr','ignored']:
 p=base/sub/'needle.txt';p.parent.mkdir();p.write_text('Needle\n')
(base/'.ignore').write_text('ignored/\n');(base/'broken').symlink_to('does-not-exist')
requests=[];categories=[]
def add(tool,args,category):requests.append(call(tool,args,len(requests)+1));categories.append(category)
for mode in ['content','count','files_with_matches']:
 for v in [True,False,0,1,-1,0.5,'true','false','1','0','TRUE',None,[],{}]:
  for key in ['-n','-i','multiline']:
   add('grep',{'pattern':'Needle','path':'multiline.txt','output_mode':mode,key:v},'bool-coercion')
 for key in ['-B','-A','-C','context','head_limit','offset']:
  for v in [-2,-0.5,0,0.5,1,2,100,1e30,'1',None,[],{}]:
   add('grep',{'pattern':'Needle','path':'a01.txt','output_mode':mode,key:v},'number-coercion')
 for pattern in ['Needle','needle','Needle.*Needle','Needle\\nNeedle','[','(?=Needle)','\\xff','^','\\z','\x00','é','(?s:Needle.*END)']:
  for path in ['invalid.txt','binary.txt','long.txt','a:b.txt','dir','missing','broken','a01.txt']:
   add('grep',{'pattern':pattern,'path':path,'output_mode':mode,'multiline':True},'rg-regex-paths')
 for include in ['*.txt,*.go','*.txt *.go','*.txt\u2003*.go','{a,b}.txt','!*.txt','[','**/*.txt','*.txt,!a01.txt',' .hidden.txt  ', '']:
  add('grep',{'pattern':'Needle','include':include,'output_mode':mode},'include')
 for offset,limit in itertools.product([-1,0,1,3,5,100],[None,-1,0,1,4,250]):
  add('grep',{'pattern':'Needle','path':'a01.txt','output_mode':mode,'offset':offset,'head_limit':limit,'context':1},'pagination')
 for type_ in ['go','txt','nonexistent-type','!go','all']:
  add('grep',{'pattern':'Needle','type':type_,'output_mode':mode},'type-error')
for context in [0,1,-1,None,'2']:
 add('grep',{'pattern':'Needle','path':'a01.txt','output_mode':'content','context':context,'-C':2,'-B':3,'-A':4},'context-precedence')
for v in [None,False,4,[],{}]:
 for key in ['pattern','path','include','type','output_mode']:
  add('grep',{'pattern':'Needle','path':'a01.txt',key:v},'malformed-strings')
patterns=['{0..255}','{0..256}','{1..256}','{1..257}','{a,b}'*8,'{a,b}'*9,'{-01..01}.txt','{+01..+03}.txt','{+01..003}.txt','{9223372036854775806..9223372036854775807}','{9223372036854775807..9223372036854775807}','{-9223372036854775808..-9223372036854775807}','{-9223372036854775808..-9223372036854775808}','{0..2..-9223372036854775808}','{2..0..-9223372036854775808}','{a,{b,c}}','{a\\,b,c}','{a{b,c}}','{a,b}{','{a,b}}','[]','[!]','[^]','[\\]','[a\\-z].txt','[[:alpha:]-z].txt','[a-[:digit:]].txt','[[:digit:]a].txt','[a-中].txt','[😀-😀].txt','@(a|!(b))','@(a|)','+(|a)','*(|a)','?(!(a|b)).txt','!(!(a)).txt','!!!@(a|b)','dir/**/**/a','dir//a','./dir/../a','dir/./a','dir/../../data/*.txt','!./dir/*.txt','/data/dir/*.txt','/datax/*.txt','a\\/b','\\!a','a\x00*','[\x00-a]','**/[[:cntrl:]]*']
rng=random.Random(701);tokens=['a','b','中','😀','*','?','[ab]','[!a]','[[:word:]]','@(a|b)','+(|a)','!(a|b)','?(a|b)','*(a|b)','{a,b}','{01..03}',r'\*',r'\?']
patterns += [rng.choice(['','dir/','**/','!','!!'])+''.join(rng.choice(tokens) for _ in range(rng.randrange(1,6)))+rng.choice(['','.txt']) for _ in range(700)]
for p in patterns:add('glob',{'pattern':p},'glob-parser')
for path in ['missing','broken','a01.txt','dir','/data/dir','/data/dir/..','/datax','']:
 for p in ['*','**','[ab]*','!*.txt','./*.txt','../*.txt']:
  add('glob',{'pattern':p,'path':path},'glob-normalization')
for v in [None,False,4,[],{}]:
 for key in ['pattern','path']:add('glob',{'pattern':'*',key:v},'glob-malformed')
def run_search(rs, binary='mcp-server', fixture=None, fake=False):
 env='PATH=/data/rg-bin:$PATH' if fake else 'RIPGREP_CONFIG_PATH=/data/rg-config'
 cmd=['docker','run','--rm','-i','--platform','linux/amd64','--network','none','-v',str(ROOT)+':/src:ro','-v',str(base)+':/data','mcp-recovery-oracle','sh','-c',f'cp /src/{binary} /tmp/server; chmod +x /tmp/server; {env} /tmp/server -workdir /data']
 p=subprocess.run(cmd,input=''.join(json.dumps(r)+'\n' for r in rs),capture_output=True,text=True,timeout=300)
 return [json.loads(l) for l in p.stdout.splitlines()],p.stderr,p.returncode
fake_specs=[];fake_requests=[]
if '--controlled-only' in sys.argv:
 requests=[];categories=[]
def fake(spec,mode='content',extra=None):
 fake_specs.append(spec);fake_requests.append(call('grep',{'pattern':'fake-'+str(len(fake_specs)-1),'path':'a01.txt','output_mode':mode,**(extra or {})},len(requests)+len(fake_specs)))
def event(path=None,lines=None,**extra): return json.dumps({'type':'match','data':{'path':path or {'text':'/data/a01.txt'},'lines':lines or {'text':'Needle\n'},'line_number':1,**extra}})
for path in [{'text':'/tmp/outside'},{'text':':abc'},{'text':'/data/ a.txt '},{'text':'/data/a01.txt','bytes':'garbage'},{'bytes':base64.b64encode(b'/data/b\xff.txt').decode()},{'bytes':'!invalid'},{'text':'','bytes':''}]:
 fake({'stdout':event(path=path)})
for lines in [{'text':'text\n','bytes':'garbage'},{'bytes':base64.b64encode(b'bad\xff\n').decode()},{'bytes':'!invalid'},{'text':'\n\n'},{'text':'  Needle  \r\n'},{'text':''}]: fake({'stdout':event(lines=lines)})
for raw in ['garbage\n'+event(),'null\n'+event(),json.dumps({'type':'begin','data':{}})+'\n'+event(),json.dumps({'type':'match','data':'wrong'}),json.dumps({'type':'match','data':{'path':{'text':3}}}),json.dumps({'type':'context','data':{'path':{'text':'/data/a01.txt'},'lines':{'text':' x\n'},'line_number':2}})]: fake({'stdout':raw})
for raw in ['3','003',' -1 ','word',':3','/tmp/x:2','/data/x:abc','/data/a:b:4','/data/x:1\n\n \t\n/data/y:2','3\nword\n4']:
 fake({'stdout':raw},'count')
for raw in ['\n\n',' /tmp/x \n\n /data/y \n','/tmp/outside\n/data/a01.txt\n/data/dir/b','/data/a01.txt\n\t \n/data/a01.txt']: fake({'stdout':raw},'files_with_matches')
for code in [0,1,2,127]:
 for err in ['', '  ordinary error \n', 'os error 11','Resource temporarily unavailable']:
  fake({'code':code,'stderr':err,'stdout':event(),'retry':True})
for args in [{'multiline':'true','-i':'1','-n':0},{'-B':2,'-A':3,'-C':4,'context':1,'include':'a,b c','type':'go'},{'head_limit':1,'offset':1}]: fake({'args':True},extra=args)
for n in [19984,19985,19986,19999,20000,20001]:
 for char in ['x','😀']: fake({'stdout':event(lines={'text':char*n+'\n'})},extra={'head_limit':1})
many='\n'.join(event(lines={'text':'line-'+str(i)+'\n'},line_number=i+1) for i in range(251))
for params in [{},{'head_limit':0},{'head_limit':-1},{'head_limit':251},{'head_limit':1,'offset':250},{'offset':251},{'offset':252}]:
 fake({'stdout':many},extra=params)
manycount='\n'.join('/data/file-'+str(i)+':'+str(i%3) for i in range(251))
for params in [{},{'head_limit':251},{'head_limit':1,'offset':250},{'offset':251}]: fake({'stdout':manycount},'count',params)
(base/'fake-specs.json').write_text(json.dumps(fake_specs));(base/'rg-bin').mkdir()
wrapper=base/'rg-bin/rg';wrapper.write_text("""#!/usr/bin/python3
import json,sys
args=sys.argv[1:];pat=next(a for a in args if a.startswith('fake-'));s=json.load(open('/data/fake-specs.json'))[int(pat[5:])]
if s.get('args'): print(json.dumps({'type':'match','data':{'path':{'text':'/data/a01.txt'},'lines':{'text':json.dumps(args)},'line_number':1}}));sys.exit(0)
if s.get('retry') and '-j' in args: s={'stdout':s.get('stdout','')}
sys.stdout.write(s.get('stdout',''));sys.stderr.write(s.get('stderr',''));sys.exit(s.get('code',0))
""");wrapper.chmod(0o755)
rebuilt=os.environ.get('ALIGNMENT_BINARY',os.environ.get('SEARCH_REBUILT','recovery/search-rebuilt'))
# One thread keeps ripgrep's directory traversal reproducible across both runs.
# The source deliberately retains the original unsorted content/count outputs.
(base/'rg-config').write_text('--threads=1\n')
orig,e1,c1=run_search(requests);reb,e2,c2=run_search(requests,binary=rebuilt)
fo,fe1,fc1=run_search(fake_requests,'mcp-server',fake=True);fr,fe2,fc2=run_search(fake_requests,rebuilt,fake=True)
requests+=fake_requests;categories+=['controlled-rg']*len(fake_requests);orig+=fo;reb+=fr;e1+=fe1;e2+=fe2;c1=c1 or fc1;c2=c2 or fc2

fail=[{'case':i+1,'category':categories[i],'request':requests[i],'original':x,'rebuilt':y} for i,(x,y) in enumerate(itertools.zip_longest(orig,reb)) if canonical(normalize(x))!=canonical(normalize(y))]
clean_stderr=lambda value:re.sub(r'^\d{4}/\d\d/\d\d \d\d:\d\d:\d\d ', '<time> ', value, flags=re.M)
if clean_stderr(e1)!=clean_stderr(e2):fail.append({'stderr_original':e1,'stderr_rebuilt':e2})
if len(orig)!=len(requests) or len(reb)!=len(requests) or c1 or c2:fail.append({'responses':[len(orig),len(reb)],'expected':len(requests),'exit_codes':[c1,c2]})
report={'cases':len(requests),'responses':[len(orig),len(reb)],'counts':{c:categories.count(c) for c in sorted(set(categories))},'failures':fail,'verified_sha256':[hashlib.sha256(json.dumps(normalize(x),sort_keys=True).encode()).hexdigest() for x in orig], 'original_stderr':e1[-2000:],'rebuilt_stderr':e2[-2000:]}
report.update(original_binary_sha256=hashlib.sha256((ROOT/'mcp-server').read_bytes()).hexdigest(),rebuilt_binary_sha256=hashlib.sha256((ROOT/rebuilt).read_bytes()).hexdigest(),external_rg_configuration='real rg: --threads=1; controlled rg: fixture PATH wrapper')
report_name='search-controlled-report.json' if '--controlled-only' in sys.argv else 'search-alignment-report.json'
(ROOT/'recovery/evidence'/report_name).write_text(json.dumps(report,ensure_ascii=False,indent=2));print('Search alignment:',len(requests),'cases;',len(fail),'failures')
for f in fail[:25]:print(json.dumps(f,ensure_ascii=False)[:3000])
sys.exit(bool(fail))
