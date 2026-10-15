"""Execute both servers in the same isolated Linux fixture."""
import pathlib, subprocess, json, os
ROOT=pathlib.Path(os.environ.get('QODER_COMPAT_ROOT',pathlib.Path(__file__).resolve().parents[2]))
IMAGE=os.environ.get('QODER_COMPAT_IMAGE','environment-manager-qoder-oracle')
def run(requests,binary='mcp-server',fixture=None):
 cmd=['docker','run','--rm','-i','--network','none','-v',str(ROOT)+':/src:ro']
 if fixture:cmd+=['-v',str(pathlib.Path(fixture).resolve())+':/data']
 cmd += [IMAGE,'sh','-c',f'cp /src/{binary} /tmp/server; chmod +x /tmp/server; /tmp/server -workdir /data']
 p=subprocess.run(cmd,input=''.join(json.dumps(r,ensure_ascii=False)+'\n' if not isinstance(r,str) else r+'\n' for r in requests),capture_output=True,text=True,timeout=240)
 return [json.loads(l) for l in p.stdout.splitlines()],p.stderr,p.returncode

def call(name,args,id=1): return {'jsonrpc':'2.0','id':id,'method':'tools/call','params':{'name':name,'arguments':args}}
