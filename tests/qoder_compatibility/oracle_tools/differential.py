"""Compare reconstructed server with the supplied executable, in isolated Linux fixtures."""
import json, pathlib, shutil, random, os, hashlib, re, sys
from probe import run,call,ROOT
OUT=ROOT/'recovery/evidence'; BASE=ROOT/'recovery/differential'

def fixture(path):
 shutil.rmtree(path,ignore_errors=True);path.mkdir(parents=True)
 files={'lines.txt':b'alpha\nbeta\ngamma\n','empty':b'','crlf.txt':b'\xef\xbb\xbfalpha\r\nbeta\r\n','repeat.txt':b'a a\n','binary':b'x\0y','mixed.txt':b'a\r\nb\r\nc\n','context.txt':b'first\n\nmatch\n \nlast\nmatch\nend\n','nested/a.txt':b'alpha\n','nested/deeper/b.go':b'package b\n','a1.txt':b'alpha\n','a2.txt':b'beta\n','.hidden.txt':b'alpha\n','.git/config':b'alpha\n','x[1].txt':b'x\n','中文.txt':'中文\n'.encode()}
 for i,(name,data) in enumerate(files.items()):
  p=path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data);os.utime(p,(1700000000+i,1700000000+i))
 (path/'link.txt').symlink_to('lines.txt');(path/'broken').symlink_to('missing')
 (path/'readonly.txt').write_text('old\n');(path/'readonly.txt').chmod(0o600)

def normalize(x):
 if isinstance(x,list): return [normalize(v) for v in x]
 if isinstance(x,dict):
  x={k:normalize(v) for k,v in x.items()}
  if 'tools' in x and isinstance(x['tools'],list):x['tools'].sort(key=lambda t:t['name'])
  return x
 if isinstance(x,str):
  x=re.sub(r'/tmp/tool-outputs/(?:bash_\d+\.log|bash-\d+\.sh|[A-Za-z_]+_\d+\.txt)(?=$|[\s:\]])','<temporary-file>',x)
  x=re.sub(r'(\A\[backgrounded\] Command did not finish within \d+s and continues running in the background\.\npid: )\d+(?=\noutput_file: )',r'\1<pid>',x)
  return x
 return x

def canonical(x):
 return json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(",",":"))

def snapshot(path):
 result={}
 for p in sorted(path.rglob('*')):
  if p.is_symlink():result[str(p.relative_to(path))]={'symlink':os.readlink(p)}
  elif p.is_file():result[str(p.relative_to(path))]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mode':oct(p.stat().st_mode&0o7777)}
  elif p.is_dir():result[str(p.relative_to(path))]={'directory':True,'mode':oct(p.stat().st_mode&0o7777)}
 return result

def cases():
 rs=['','{','[]','null',{}, {'jsonrpc':'1.0','id':0,'method':'ping'}]
 for method in ['initialize','ping','tools/list','nonsense']:
  for ident in [1,0,'x',None]:rs.append({'jsonrpc':'2.0','id':ident,'method':method})
 rs += [{'jsonrpc':'2.0','id':99,'method':'tools/call','params':p} for p in [None,[],{}, {'name':'absent'}, {'name':'read','arguments':[]}, {'name':'read'}]]
 for name in ['read','write','edit','bash','glob','grep','file_read','file_write','file_edit','run_in_terminal','glob_run','grep_run']:
  for args in [{},{'file_path':None,'command':None,'pattern':None},{'file_path':42,'command':42,'pattern':42}]:rs.append(call(name,args))
 for p in ['/data/lines.txt','lines.txt','/data/empty','/data/crlf.txt','/data/binary','/data/nested','/data/linex.txt','/data/unknown.txt','/dev/null','/dev/zero','//data/lines.txt','\\\\server\\x','/data/fake.png']:
  rs.append(call('read',{'file_path':p}))
 for start in [-3,0,1,2,3,100,'1',True,1.9,None]:
  for end in [-1,0,1,3,99,'2',None]:rs.append(call('read',{'file_path':'/data/lines.txt','start_line':start,'end_line':end}))
 rs.append(call('file_read',{'file_path':'/data/lines.txt','startLine':1,'endLine':2}))
 for name,new in [('new.txt','hello\nworld\n'),('empty-new',''),('lines.txt','alpha\ngamma\n'),('crlf.txt','new\ntext\n'),('mixed.txt','a\nb\nc\n'),('readonly.txt','new\n'),('link.txt','linked\n'),('nested','fail'),('broken','fixed')]:rs.append(call('write',{'file_path':name,'content':new}))
 rng=random.Random(923)
 for i in range(100):
  a='\n'.join(rng.choice(['a','b','c','','d']) for _ in range(rng.randrange(0,25)))+rng.choice(['','\n'])
  b='\n'.join(rng.choice(['a','b','c','','d']) for _ in range(rng.randrange(0,25)))+rng.choice(['','\n'])
  rs += [call('write',{'file_path':f'diff/{i}.txt','content':a}),call('write',{'file_path':f'diff/{i}.txt','content':b})]
 for args in [{'file_path':'new-edit','old_string':'','new_string':'created\n'}, {'file_path':'missing','old_string':'a','new_string':'b'}, {'file_path':'repeat.txt','old_string':'a','new_string':'b'}, {'file_path':'new.txt','old_string':'world','new_string':'everyone'}, {'file_path':'new.txt','old_string':'missing','new_string':'x'}, {'file_path':'empty','old_string':'','new_string':'x'}, {'file_path':'nested','old_string':'a','new_string':'b'}]:rs.append(call('edit',args))
 # Search in immutable single file to avoid ripgrep thread scheduling differences.
 for mode in ['content','count','files_with_matches','bad']:
  for pat in ['match','^','notfound','[']:
   rs.append(call('grep',{'path':'context.txt','pattern':pat,'output_mode':mode}))
 for options in [{'head_limit':x} for x in [-1,0,1,2,100,'1',True]]+ [{'offset':x} for x in [-1,0,1,2,100]]+ [{'context':x} for x in [0,1,2,-1]]+[{'-B':2},{'-A':2},{'-n':False},{'-n':None},{'context':1,'-B':2},{'-i':True},{'multiline':True}]:
  rs.append(call('grep',dict(path='context.txt',pattern='match',output_mode='content',**options)))
 for pattern in ['*','**','*.txt','**/*.txt','nested/*','nested/**','!*.txt','!!*.txt','@(a1|a2).txt','!(a1).txt','+(a|1).txt','?(a)1.txt','*(a)1.txt','a[12].txt','a[!2].txt','a[[:digit:]].txt','a{1,2}.txt','a{1..2}.txt','{nested,missing}/**','[','[]','[z-a]','[[:bogus:]]','{a,b','{1..999}','{1..2..0}','/data/*.txt','/tmp/*.txt','./*.txt',r'x\[1\].txt']:
  rs.append(call('glob',{'pattern':pattern}))
 for command in ['printf hi','echo hi','true','exit 4','echo hi; exit 4','pwd','printf "%s" "$FOO"','cat','vim x','python','echo hi | cat','echo hi && less x','echo "; vim"','printf "你好"']:
  rs.append(call('bash',{'command':command,'env':{'FOO':'injected'}}))
 for args in [{'command':'true','env':{'X':42}},{'command':'true','env':{'A-B':'x'}},{'command':'pwd','work_dir':'nested'},{'command':'pwd','work_dir':'missing'},{'command':'true','on_timeout':'bad'}]:rs.append(call('bash',args))
 # Unique IDs let notifications or omitted responses be detected.
 for i,r in enumerate(rs):
  if isinstance(r,dict) and r.get('method')=='tools/call' and 'id' in r:r['id']=1000+i
 return rs

if __name__=='__main__':
 BASE.mkdir(exist_ok=True);rs=cases(); a=BASE/'original';b=BASE/'rebuilt';fixture(a);fixture(b)
 original,err1,c1=run(rs,fixture=a); rebuilt,err2,c2=run(rs,binary='recovery/rebuilt-mcp-server',fixture=b)
 from itertools import zip_longest
 failures=[]
 if len(original)!=448 or len(rebuilt)!=448:failures.append({'response_counts':[len(original),len(rebuilt)],'expected':448})
 if c1 or c2:failures.append({'exit_codes':[c1,c2]})
 for index,(left,right) in enumerate(zip_longest(original,rebuilt)):
  if canonical(normalize(left))!=canonical(normalize(right)):
   key=(left or right).get('id')
   failures.append({'response_index':index,'id':key,'request':next((r for r in rs if isinstance(r,dict) and r.get('id')==key),None),'original':left,'rebuilt':right})
 sa,sb=snapshot(a),snapshot(b)
 for key in dict.fromkeys([*sa,*sb]):
  if sa.get(key)!=sb.get(key):failures.append({'file':key,'original':sa.get(key),'rebuilt':sb.get(key)})
 report={'requests':len(rs),'responses_original':len(original),'responses_rebuilt':len(rebuilt),'exit_codes':[c1,c2],'failures':failures,'normalization':['tools/list order (Go map iteration)','temporary output paths','background PIDs'],'stderr_original':err1,'stderr_rebuilt':err2}
 (OUT/'differential-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print(json.dumps({k:v for k,v in report.items() if k not in ['stderr_original','stderr_rebuilt','failures']}));print('FAILURES',len(failures))
 for failure in failures[:30]:print(json.dumps(failure,ensure_ascii=False)[:2200])
 sys.exit(bool(failures))
