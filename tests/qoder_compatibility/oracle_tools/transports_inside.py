import subprocess,json,socket,threading,time,urllib.request,urllib.error,pathlib,sys,re
binary=sys.argv[1]
results=[]
def req(method='ping',ident=1):return {'jsonrpc':'2.0','id':ident,'method':method}
def request(method,path,body=None):
 r=urllib.request.Request('http://127.0.0.1:32191'+path,data=body,method=method)
 try:res=urllib.request.urlopen(r,timeout=5)
 except urllib.error.HTTPError as e:res=e
 text=res.read().decode();ct=res.headers.get('Content-Type');
 try:text=json.loads(text)
 except:pass
 return {'status':res.status,'content_type':ct,'body':text}
p=subprocess.Popen([binary,'-transport','http','-addr','127.0.0.1:32191','-workdir','/data'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
for _ in range(100):
 try:
  c=socket.create_connection(('127.0.0.1',32191),.1);c.close();break
 except:time.sleep(.03)
for method,path,body in [('GET','/mcp',None),('GET','/',None),('POST','/mcp',b'{'),('POST','/mcp',b'null'),('POST','/mcp',b''),('PUT','/mcp',b'{}'),('POST','/mcp',json.dumps(req()).encode()),('POST','/mcp',json.dumps(req(ident=None)).encode()),('POST','/mcp',json.dumps(req('initialize')).encode()),('POST','/mcp',json.dumps(req('invalid')).encode()),('POST','/mcp/',b'{}')]:results.append({'http_request':[method,path,body.decode() if body else None],'response':request(method,path,body)})
p.terminate();p.communicate(timeout=5)
for i,payload in enumerate([b'{"env":{"FOO":"runtime","CI":"runtime","LANG":"runtime"}}',b'{}',b'{"env":null}',b'{"env":{"FOO":123}}',b'{',b'null',b'{"env":{"FOO":"'+b'x'*(1<<20)+b'"}}',b'{"env":{"FOO":"a=b","BAD-NAME":"allowed?"}}',b'{"env":{"FOO":"first"}} {"env":{"FOO":"second"}}']):
 path=f'/tmp/env-{i}.sock';s=socket.socket(socket.AF_UNIX);s.bind(path);s.listen();s.settimeout(5)
 def listener():
  try:
   c,_=s.accept()
   try:c.sendall(payload)
   except BrokenPipeError:pass
   c.close()
  except:pass
 t=threading.Thread(target=listener);t.start()
 r={'jsonrpc':'2.0','id':i+1,'method':'tools/call','params':{'name':'bash','arguments':{'command':'printf "%s" "$FOO/$CI/$LANG"','env':{'FOO':'call'}}}}
 out=subprocess.run([binary,'-env-socket',path,'-workdir','/data'],input=json.dumps(r)+'\n',text=True,capture_output=True,timeout=10);t.join();s.close();pathlib.Path(path).unlink();results.append({'env_case':i,'response':json.loads(out.stdout),'stderr':out.stderr.split(' INFO ')[-1].strip(),'code':out.returncode})
# A missing socket exercises startup and error wrapping without timing normalization.
r['id']=99
out=subprocess.run([binary,'-env-socket','/tmp/missing-env.sock','-workdir','/data'],input=json.dumps(r)+'\n',text=True,capture_output=True,timeout=10);results.append({'env_case':'missing','response':json.loads(out.stdout),'code':out.returncode})
for argv in [['-transport','bad'],['-h'],['-core-addr','127.0.0.1:9']]:
 out=subprocess.run([binary,*argv],input='',text=True,capture_output=True,timeout=10);err=re.sub(r'^\d{4}/\d\d/\d\d \d\d:\d\d:\d\d ','',out.stderr,flags=re.M);results.append({'argv':argv,'stdout':out.stdout,'stderr':err,'code':out.returncode})
print(json.dumps(results,ensure_ascii=False))
