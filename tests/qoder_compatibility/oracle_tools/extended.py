import pathlib,json,hashlib,shutil,sys,os,itertools
from probe import ROOT,run,call
from differential import canonical,normalize,fixture,snapshot
base=ROOT/'recovery/extended';base.mkdir(exist_ok=True);a=base/'original';b=base/'rebuilt';fixture(a)
for p in (ROOT/'recovery/images').glob('*'):shutil.copyfile(p,a/p.name)
(a/'wrong.PNG').write_bytes((a/'tiny.jpg').read_bytes());(a/'invalid.png').write_bytes(b'not an image')
(a/'long.txt').write_text('中'*60000+'\n');(a/'big.txt').write_bytes(b'x'*(10*1024*1024+1));(a/'long-grep.txt').write_text('match '+('a'*450)+'\n'*0 if False else ('match '+('a'*450)+'\n')*300)
for i in range(110):
 p=a/'many'/f'{i:03d}.txt';p.parent.mkdir(exist_ok=True);p.write_text('x\n');os.utime(p,(1600000000+i,1600000000+i))
rs=[]
for p in a.glob('*'):
 if p.suffix.lower() in ['.png','.jpg','.gif']:rs.append(call('read',{'file_path':'/data/'+p.name}))
for p in ['long.txt','big.txt']:rs.append(call('read',{'file_path':'/data/'+p,'start_line':0,'end_line':1}))
rs += [call('grep',{'path':'long-grep.txt','pattern':'match','output_mode':m, 'head_limit':h,'offset':o}) for m in ['content','count','files_with_matches'] for h in [0,1,250,300] for o in [0,250,999]]
rs += [call('glob',{'pattern':p,'path':'many'}) for p in ['*','**','{000..109}.txt','!000.txt']]
rs += [call('write',{'file_path':'long-diff.txt','content':'中'*60000+'\n'}),call('file_write',{'file_path':'alias.txt','content':'alias\n'}),call('file_read',{'file_path':'/data/alias.txt','startLine':0,'endLine':1}),call('file_edit',{'file_path':'alias.txt','old_string':'alias','new_string':'edited'})]
old='\n'.join(str(i) for i in range(80));new=old.replace('\n10\n','\nten\n').replace('\n70\n','\nseventy\n')
rs += [call('write',{'file_path':'hunks.txt','content':old}),call('write',{'file_path':'hunks.txt','content':new}),call('edit',{'file_path':'empty','old_string':'','new_string':'created'}),call('edit',{'file_path':'alias.txt','old_string':'','new_string':'overwrite'})]
for cmd in ["python3 -c 'print(\"x\"*40000,end=\"\")'","python3 -c 'print(\"中\"*20000,end=\"\")'",'printf begin; sleep 10','trap "echo stopped; exit 0" TERM; sleep 10','trap "" TERM; sleep 10','printf begin; sleep 2; echo done']:
 rs.append(call('bash',{'command':cmd,'timeout':1,'on_timeout':'background' if 'sleep 2;' in cmd else 'kill'}))
rs += [call('run_in_terminal',{'command':'echo alias','timeout':0,'wait_time':1,'session_id':'bad'}),call('bash',{'command':'printf "%s" "$CI/$LANG/$TERM"','env':{'CI':'override'}}),call('bash',{'command':'env','env':{'X':'a\0b'}})]
rs.append(call('bash',{'command':"sleep 3; python3 -c 'import pathlib,hashlib; print(sorted((p.name.split(\"_\")[0],len(p.read_bytes()),hashlib.sha256(p.read_bytes()).hexdigest()) for p in pathlib.Path(\"/tmp/tool-outputs\").glob(\"*\") if p.suffix in [\".log\",\".txt\"])))'"}))
shutil.rmtree(b,ignore_errors=True);shutil.copytree(a,b,symlinks=True)
for i,r in enumerate(rs):r['id']=i+1
one,e1,c1=run(rs,fixture=a);two,e2,c2=run(rs,binary='recovery/rebuilt-mcp-server',fixture=b)
fail=[]
if len(one)!=len(rs) or len(two)!=len(rs):fail.append({'response_counts': [len(one),len(two)], 'expected':len(rs)})
if c1 or c2:fail.append({'exit_codes':[c1,c2]})
for i,(x,y) in enumerate(itertools.zip_longest(one,two)):
 if canonical(normalize(x))!=canonical(normalize(y)):fail.append({'request':rs[i],'original':x,'rebuilt':y})
sa,sb=snapshot(a),snapshot(b)
for key in dict.fromkeys([*sa,*sb]):
 if sa.get(key)!=sb.get(key):fail.append({'file':key,'original':sa.get(key),'rebuilt':sb.get(key)})
verified=[]
for request,x,y in zip(rs,one,two):
 if canonical(normalize(x))==canonical(normalize(y)):
  item={'id':request['id'],'tool':request['params']['name'],'response_sha256':hashlib.sha256(json.dumps(normalize(x),ensure_ascii=False,sort_keys=True).encode()).hexdigest()}
  if request['params']['name']=='read':item['path']=request['params']['arguments']['file_path']
  for block in x.get('result',{}).get('content',[]):
   if block.get('type')=='image':item['image_base64_sha256']=hashlib.sha256(block['data'].encode()).hexdigest()
  verified.append(item)
report={'verified':verified,'requests':len(rs),'responses':[len(one),len(two)],'exit_codes':[c1,c2],'failures':fail,'stderr_original':e1,'stderr_rebuilt':e2}
(ROOT/'recovery/evidence/extended-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print('Requests',len(rs),'failures',len(fail))
for f in fail:
 for k in ['original','rebuilt']:
  if isinstance(f.get(k),dict) and 'result' in f[k]:
   for block in f[k]['result'].get('content',[]):
    if 'data' in block:block['data']='SHA256:'+hashlib.sha256(block['data'].encode()).hexdigest()
 print(json.dumps(f,ensure_ascii=False)[:4000])
sys.exit(bool(fail))
