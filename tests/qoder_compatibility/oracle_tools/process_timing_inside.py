"""Runs inside the network-isolated Linux oracle container. Durations are classified, never compared bytewise."""
import http.client,json,pathlib,socket,subprocess,time,os,re,sys
binary=sys.argv[1]; result={'timing':[]};server=subprocess.Popen([binary,'-workdir','/data'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
commands=[('timeout-seconds','sleep 6',1,'kill',0.8,2.5),('term-grace','trap "" TERM; sleep 6',1,'kill',2.7,4.5),('background-release','sleep 3',1,'background',0.8,2.5)]
for i,(name,cmd,to,mode,minimum,maximum) in enumerate(commands,1):
 req={'jsonrpc':'2.0','id':i,'method':'tools/call','params':{'name':'bash','arguments':{'command':cmd,'timeout':to,'on_timeout':mode}}};t=time.monotonic();server.stdin.write(json.dumps(req)+'\n');server.stdin.flush();response=json.loads(server.stdout.readline());duration=time.monotonic()-t
 result['timing'].append({'case':name,'duration_seconds':duration,'within_bounds':minimum<=duration<=maximum,'bounds':[minimum,maximum],'response':response})
time.sleep(3);server.stdin.close();server.wait(timeout=5);result['stdio_stderr']=server.stderr.read()
server=subprocess.Popen([binary,'-transport','http','-addr','127.0.0.1:32192','-workdir','/data'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
try:
 for _ in range(100):
  try:c=socket.create_connection(('127.0.0.1',32192),.1);c.close();break
  except OSError:time.sleep(.05)
 ready=pathlib.Path('/data/http-cancel-ready');late=pathlib.Path('/data/http-cancel-late');ready.unlink(missing_ok=True);late.unlink(missing_ok=True)
 req={'jsonrpc':'2.0','id':9,'method':'tools/call','params':{'name':'bash','arguments':{'command':"printf '%s' $$ > /data/http-cancel-ready; sleep 9; printf late > /data/http-cancel-late",'timeout':600}}};body=json.dumps(req).encode();c=socket.create_connection(('127.0.0.1',32192),2);c.sendall(b'POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)
 for _ in range(100):
  if ready.exists() and ready.read_text():break
  time.sleep(.05)
 pid=int(ready.read_text());t=time.monotonic();c.shutdown(socket.SHUT_RDWR);c.close();running=True
 for _ in range(80):
  try:running=pathlib.Path('/proc/%d/stat'%pid).read_text().split()[2] not in ('Z','X')
  except (FileNotFoundError,ProcessLookupError):running=False
  if not running:break
  time.sleep(.05)
 result['http_cancellation']={'child_stopped':not running,'late_write':late.exists(),'duration_seconds':time.monotonic()-t}
finally:
 server.terminate();server.wait(timeout=5);result['http_stderr']=server.stderr.read()
print(json.dumps(result))
