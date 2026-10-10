"""Protocol/CLI/HTTP/environment oracle comparison. No binary patching.
Host: python3 recovery/tools/protocol_alignment.py [rebuilt relative path]
"""
import json, pathlib, subprocess, sys, re, os, socket, threading, time, http.client, itertools, random, hashlib
from differential import canonical
ROOT=pathlib.Path(__file__).resolve().parents[2]
def clean(x):
 if isinstance(x,str):return x
 if isinstance(x,list):return [clean(y) for y in x]
 if isinstance(x,dict):
  x={k:(re.sub(r'^\d{4}/\d\d/\d\d \d\d:\d\d:\d\d ', '<time> ',v,flags=re.M) if k=='stderr' and isinstance(v,str) else clean(v)) for k,v in x.items()}
  if isinstance(x.get('tools'),list):x['tools']=sorted(x['tools'],key=lambda y:y['name'])
  return x
 return x
def corpus():
 rows=[]
 ids=[None,0,1,-1,1.5,'', '字符串',True,False,[],{},[1,None],{'x':[False]},9007199254740993]
 methods=['ping','initialize','tools/list','tools/call','notifications/initialized','',None,9,{},[],'PING','未知']
 for i,m in itertools.product(ids,methods):
  rows.append(json.dumps({'jsonrpc':'2.0','id':i,'method':m},ensure_ascii=False))
 for version in ['',None,1,False,[],{},'2.1','2.0']:
  for i in [None,0,False,{},'x']:
   rows.append(json.dumps({'jsonrpc':version,'id':i,'method':'ping'}))
 for param in [None,[],1,'x',False,{}, {'name':None},{'name':1},{'name':{}},{'name':'missing'},{'name':'read'},{'name':'read','arguments':None},{'name':'read','arguments':[]},{'name':'read','arguments':1},{'name':'read','arguments':{'file_path':None}}]:
  rows.append(json.dumps({'jsonrpc':'2.0','id':1,'method':'tools/call','params':param}))
 for field in ['jsonrpc','id','method','params']:
  for value in [None,False,0,{},[], 'ping']:
   req={'jsonrpc':'2.0','id':1,'method':'ping','params':{}};req[field]=value
   rows.append(json.dumps(req))
 rows += ['', ' ', '{}','null','true','false','0','"x"','[]','[{}]', '{', '{"jsonrpc":"2.0","id":1,"method":"ping"}{}',
  '{"jsonrpc":"2.0","id":1,"method":"ping","id":null}',
  '{"jsonrpc":"invalid","jsonrpc":"2.0","id":1,"method":"ping"}',
  '{"JSONRPC":"2.0","ID":false,"METHOD":"ping"}',
  '{"jsonrpc":"2.0","id":1e999,"method":"ping"}',
  '{"jsonrpc":"2.0","id":-0,"method":"ping"}',
  '{"jsonrpc":"2.0","id":"\\ud800","method":"ping"}',
  '{"jsonrpc":"2.0","id":1,"method":"\\ud800"}',
  '{"jsonrpc":"2.0","id":1,"method":"ping","extra":"'+('x'*70000)+'"}',
  '\ufeff{"jsonrpc":"2.0","id":1,"method":"ping"}']
 rng=random.Random(1729)
 for _ in range(500):
  req={'jsonrpc':rng.choice(['2.0','',None,0]),'id':rng.choice(ids),'method':rng.choice(methods),'params':rng.choice([None,{},[],{'name':'read','arguments':{'file_path':None}}, {'name':'missing'}])}
  rows.append(json.dumps(req,ensure_ascii=False))
 return rows
def inside(binary):
 out=[]
 def execute(label, data='', args=(), env=None, count=1):
  p=subprocess.run([binary,*args],input=data.encode() if isinstance(data,str) else data,capture_output=True,env=env,timeout=30)
  try:body=[json.loads(l) for l in p.stdout.splitlines()]
  except ValueError:body=p.stdout.decode(errors='replace')
  out.append({'label':label,'case_count':count,'code':p.returncode,'stdout':body,'stderr':p.stderr.decode(errors='replace')})
 # Non-EOF read error: Go os.File.Read on a directory returns EISDIR.
 fd=os.open('/data',os.O_RDONLY)
 try:
  p=subprocess.run([binary],stdin=fd,capture_output=True,timeout=5)
  out.append({'label':'stdin-read-error-directory','case_count':1,'code':p.returncode,'stdout':p.stdout.decode(errors='replace'),'stderr':p.stderr.decode(errors='replace')})
 finally:os.close(fd)
 rows=corpus();execute('json-shape-and-seeded-mutation', '\n'.join(rows)+'\n',count=len(rows))
 ping='{"jsonrpc":"2.0","id":1,"method":"ping"}'
 for label,data in [('partial-eof',ping),('partial-after-valid',ping+'\n'+ping),('crlf',ping+'\r\n'),('nul',b'\x00\n'),('invalid-utf8',b'{"jsonrpc":"2.0","id":1,"method":"\xff"}\n')]:execute(label,data)
 for args in [('-help',),('-h',),('-no-such-flag',),('-transport',),('-transport=HTTP',),('-transport=',),('-transport=http','-addr=bad::address'),('-transport=http','-addr=127.0.0.1:-1'),('--', '-transport=http'),('ignored-positional','-transport=http'),('-core-addr=http://bad',),('-env-socket=  ',)]:execute('cli:'+repr(args),ping+'\n',args)
 for wd in ['', '/tmp','/missing/path',' ','/data']:
  env=dict(os.environ,WORK_DIR=wd)
  execute('workdir-env:'+repr(wd),json.dumps({'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'bash','arguments':{'command':'pwd'}}})+'\n',('-workdir','/data'),env)
 # One HTTP process; malformed request body read branches and routing.
 p=subprocess.Popen([binary,'-transport=http','-addr=127.0.0.1:32282'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 for _ in range(200):
  try:
   s=socket.create_connection(('127.0.0.1',32282),.2);s.close();break
  except OSError:time.sleep(.02)
 def http_case(label,method,path,body='',headers=None):
  c=http.client.HTTPConnection('127.0.0.1',32282,timeout=5);c.request(method,path,body,headers or {});r=c.getresponse();b=r.read().decode(errors='replace')
  try:b=json.loads(b)
  except ValueError:pass
  out.append({'label':'http:'+label,'case_count':1,'status':r.status,'headers':{k:v for k,v in r.getheaders() if k.lower()!='date'},'body':b});c.close()
 for method,path in itertools.product(['GET','HEAD','OPTIONS','PUT','PATCH','DELETE','POST'],['/mcp','/mcp/','/','//mcp','/x/../mcp','/mcp?x=y']):http_case(method+path,method,path,ping)
 for i,b in enumerate(rows[:30]+rows[-25:]):http_case('body:'+str(i),'POST','/mcp',b.encode(),{'Content-Type':'not/json'})
 for label,raw in [('short-body',b'POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{}'),('bad-chunk',b'POST /mcp HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nZZ\r\n')]:
  s=socket.create_connection(('127.0.0.1',32282),5);s.sendall(raw);s.shutdown(socket.SHUT_WR);chunks=[]
  while True:
   b=s.recv(65536)
   if not b:break
   chunks.append(b)
  response=b''.join(chunks).decode(errors='replace');response=re.sub(r'Date: [^\r]+\r\n','',response)
  out.append({'label':'http:'+label,'case_count':1,'raw':response});s.close()
 p.terminate();p.communicate(timeout=5)
 # Actual Unix snapshot streams, sizes immediately below/at/above LimitedReader cap.
 snapshots=[b'',b'null',b'{}',b'{"env":null}',b'{"env":[]}',b'{"env":{"":"bad","GOOD":"yes"}}',b'{"ENV":{"GOOD":"upper"}}',b'{"env":{"GOOD":null}}',b'{"env":{"GOOD":true}}',b'{"env":{"GOOD":"yes"}} trailing',b'{"env":{"GOOD":"yes"}} {"env":{"GOOD":"no"}}',b'[]',b'"x"',b'\xef\xbb\xbf{}']
 for size in [1048575,1048576,1048577,1048578]:
  prefix=b'{"env":{"GOOD":"';suffix=b'"}}';snapshots.append(prefix+b'x'*(size-len(prefix)-len(suffix))+suffix)
 snapshots += [b' '*1048576+b'{}', b'{"env":{"BAD=KEY":"x"}}',b'{"env":{"BAD\\u0000KEY":"x"}}']
 for i,data in enumerate(snapshots):
  path='/tmp/protocol-env.sock'
  try:os.unlink(path)
  except FileNotFoundError:pass
  listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);listener.bind(path);listener.listen(1)
  def serve(payload=data):
   conn,_=listener.accept()
   try:conn.sendall(payload)
   except (BrokenPipeError,ConnectionResetError):pass
   finally:conn.close();listener.close()
  t=threading.Thread(target=serve,daemon=True);t.start()
  args={'command':"printf 'GOOD=%s;CI=%s' \"${GOOD:0:10}\" \"$CI\""}
  if i==5:args['env']={'':'drop','GOOD':'override','CI':'custom'}
  execute('env-stream:'+str(i),json.dumps({'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'bash','arguments':args}})+'\n',('-env-socket',path));t.join(2)
 for envpath in ['/tmp/no-such-environment.sock','  ']:
  execute('env-connect:'+repr(envpath),json.dumps({'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'bash','arguments':{'command':'printf ok'}}})+'\n',('-env-socket',envpath))
 # Successful dial followed by read deadline expiration.
 path='/tmp/protocol-timeout.sock'
 try:os.unlink(path)
 except FileNotFoundError:pass
 listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);listener.bind(path);listener.listen(1)
 def stalled():
  conn,_=listener.accept();time.sleep(.7);conn.close();listener.close()
 t=threading.Thread(target=stalled,daemon=True);t.start()
 execute('env-read-timeout',json.dumps({'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'bash','arguments':{'command':'printf ok'}}})+'\n',('-env-socket',path));t.join(2)
 print(json.dumps(out,ensure_ascii=False))
def host():
 rebuilt=sys.argv[1] if len(sys.argv)>1 else os.environ.get('ALIGNMENT_BINARY','recovery/protocol-rebuilt')
 fixture=ROOT/'recovery/protocol-alignment';fixture.mkdir(exist_ok=True)
 results=[]
 for binary in ['mcp-server',rebuilt]:
  args=['docker','run','--rm','--platform','linux/amd64','--network','none','-v',str(ROOT)+':/src:ro','-v',str(fixture)+':/data','mcp-recovery-oracle','sh','-c',f'cp /src/{binary} /tmp/server; chmod +x /tmp/server; python3 /src/recovery/tools/protocol_alignment.py --inside /tmp/server']
  p=subprocess.run(args,capture_output=True,text=True,timeout=180)
  if p.returncode:raise RuntimeError(p.stderr)
  results.append(json.loads(p.stdout))
 failures=[{'original':a,'rebuilt':b} for a,b in itertools.zip_longest(*results) if canonical(clean(a))!=canonical(clean(b))]
 report={'cases':sum(x['case_count'] for x in results[0]),'groups':len(results[0]),'failures':failures,'original':results[0],'rebuilt':results[1]}
 (ROOT/'recovery/evidence/protocol-alignment-report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False))
 print('Protocol alignment cases',report['cases'],'groups',report['groups'],'failures',len(failures))
 for f in failures:print(json.dumps(f,ensure_ascii=False)[:3000])
 sys.exit(bool(failures))
if __name__=='__main__':
 if len(sys.argv)>1 and sys.argv[1]=='--inside':inside(sys.argv[2])
 else:host()
