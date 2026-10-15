"""Differential boundary/failure corpus; original ELF is the behavioral oracle."""
import base64, hashlib, itertools, json, os, pathlib, random, re, shutil, struct, subprocess, sys, zlib
from probe import ROOT, call, run
from differential import canonical,normalize, snapshot
BASE=ROOT/'recovery/file-alignment'
BINARY=os.environ.get('ALIGNMENT_BINARY','recovery/file-rebuilt')
MODULE_CACHE=pathlib.Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip())
WEBP_FILES=sorted((MODULE_CACHE/'golang.org/x/image@v0.16.0/testdata').glob('*.webp'))
if not WEBP_FILES:raise RuntimeError('pinned x/image WebP fixtures missing; run go mod download')
fail=[]; verified=[]; groups=[]

def norm(x):
    x=normalize(x)
    if isinstance(x,str):return re.sub(r'\.tmp\.([^/\s:]+)\.\d+',r'.tmp.\1.<random>',x)
    if isinstance(x,list):return [norm(v) for v in x]
    if isinstance(x,dict):return {k:norm(v) for k,v in x.items()}
    return x

def invoke(requests,binary,fixture,setup='',options=()):
    if not setup and not options:return run(requests,binary=binary,fixture=fixture)
    cmd=['docker','run','--rm','--platform','linux/amd64','--network','none','-i','-v',f'{ROOT}:/src:ro','-v',f'{fixture}:/data',*[str(o).replace('{fixture}',str(fixture)) for o in options],'mcp-recovery-oracle','sh','-c',f'cp /src/{binary} /tmp/server; chmod 755 /tmp/server; {setup}']
    out=subprocess.run(cmd,input=''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in requests),text=True,capture_output=True,timeout=180)
    return [json.loads(s) for s in out.stdout.splitlines() if s],out.stderr,out.returncode

def container_snapshot(p):
    # macOS bind mounts can deny reads even to container root. Temporarily enable
    # owner reads on host, then restore and compare the original mode values.
    modes={}
    for f in p.rglob('*'):
        if f.is_file() and not f.is_symlink():
            mode=f.stat().st_mode&0o7777;modes[str(f.relative_to(p))]=mode
            if not mode&0o400:f.chmod(mode|0o400)
    try:
        result=snapshot(p)
        for name,mode in modes.items():result[name]['mode']=oct(mode)
        return result
    finally:
        for name,mode in modes.items():(p/name).chmod(mode)

def group(name,requests,fixture_builder,setup='',options=()):
    a=BASE/name/'original';b=BASE/name/'rebuilt'

    if a.parent.exists():
        for root,dirs,files in os.walk(a.parent):
            os.chmod(root,0o777)
            for f in files:
                path=pathlib.Path(root)/f
                if not path.is_symlink():path.chmod(0o666)
    shutil.rmtree(a.parent,ignore_errors=True);a.mkdir(parents=True)
    fixture_builder(a);shutil.copytree(a,b,symlinks=True)
    for i,r in enumerate(requests):r['id']=i+1
    one,e1,c1=invoke(requests,'mcp-server',a,setup,options);two,e2,c2=invoke(requests,BINARY,b,setup,options)
    if len(one)!=len(requests) or len(two)!=len(requests) or c1 or c2:fail.append({'group':name,'counts':[len(one),len(two)],'expected':len(requests),'exits':[c1,c2],'stderr':[e1,e2]})
    for i,(x,y) in enumerate(itertools.zip_longest(one,two)):
        if canonical(norm(x))!=canonical(norm(y)):
            f={'group':name,'request':requests[i],'original':x,'rebuilt':y};fail.append(f);print(json.dumps(f,ensure_ascii=False)[:4000],flush=True)
        else:verified.append({'group':name,'id':i+1,'response_sha256':hashlib.sha256(json.dumps(norm(x),sort_keys=True,ensure_ascii=False).encode()).hexdigest()})
    sa,sb=container_snapshot(a),container_snapshot(b)
    if sa!=sb:fail.append({'group':name,'snapshot_differences':{k:[sa.get(k),sb.get(k)] for k in set(sa)|set(sb) if sa.get(k)!=sb.get(k)}})
    groups.append({'name':name,'requests':len(requests),'responses':[len(one),len(two)],'exit_codes':[c1,c2]})
    print(name,len(requests),'cumulative failures',len(fail),flush=True)

def chunk(kind,data):return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data)&0xffffffff)
def png(w,h,valid=True):
    # Solid RGB fixtures compress small; invalid huge dimensions are header-only.
    raw=zlib.compress((b'\0'+b'\x21\x42\x63'*w)*h) if valid else b''
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',w,h,8,2,0,0,0))+chunk(b'IDAT',raw)+chunk(b'IEND',b'')

def fixture(p):
    for f in WEBP_FILES:shutil.copyfile(f,p/f.name)
    for name,data in {'empty':b'','one':b'a','newline':b'\n','crlf':b'a\r\nb\r\n','lines':b'a\nb\nc\n','bom':b'\xef\xbb\xbfa\n','invalid-utf':b'\xffa\xf0\x80\n','nul-8191':b'a'*8191+b'\0','nul-8192':b'a'*8192+b'\0','blocker':b'x'}.items():(p/name).write_bytes(data)
    (p/'folder').mkdir();(p/'loop').symlink_to('loop');(p/'dangling').symlink_to('missing-target');(p/'link').symlink_to('lines')
    for name in ['ab.txt','abcdef.TXT','ABC.txt','a','αβγ.txt','foo.tar.gz']:(p/name).write_text(name)
    for w,h in [(1,1),(1999,1),(2000,1),(2001,1),(1,2001),(2000,2000),(2501,17)]:
        (p/f'{w}x{h}.PNG').write_bytes(png(w,h))
    for w,h in [(0,1),(1,0),(50000001,1),(10000,5001),(2147483647,2147483647)]:
        (p/f'bad-{w}x{h}.png').write_bytes(png(w,h,False))
    for ext in ['png','jpg','gif','webp','jpeg']:
        (p/f'invalid.{ext}').write_bytes(b'not an image')
    for ext in ['png','jpg','gif']:
        src=ROOT/'recovery/images'/f'tiny.{ext}'
        if src.exists():
            data=src.read_bytes();(p/f'mismatch-{ext}.webp').write_bytes(data)
            for n in [0,1,8,24,len(data)//2,len(data)-1]:(p/f'truncated-{ext}-{n}.{ext}').write_bytes(data[:n])
    (p/'padded.png').write_bytes(png(2,3)+b'\0'*(3932161))
    (p/'ten-mib').write_bytes(b'a'*(10*1024*1024));(p/'ten-mib-plus').write_bytes(b'a'*(10*1024*1024+1))
    for name,text in [('read-limit','中'*49997),('read-limit-plus','中'*49998),('preview','中'*800+'\n'+'x'*53000)]: (p/name).write_text(text)

rs=[call('read',{'file_path':'/data/'+f.name}) for f in WEBP_FILES]
for name in ['empty','one','newline','crlf','lines','bom','invalid-utf','nul-8191','nul-8192']:
    for start,end in itertools.product([None,-9,0,1,2,3,4,99,0.9,1.9,'1',True],repeat=2):
        args={'file_path':'/data/'+name}
        if start is not None:args['start_line']=start
        if end is not None:args['end_line']=end
        rs.append(call('read',args))
for name in ['folder','loop','dangling','link','blocker/child','a\0b','x'*260,'absent','ab.tx','ABC.tx','αβγ.tx','foo.tar.g','/dev/null','/dev/zero','//data/lines']:
    rs.append(call('read',{'file_path':name if name.startswith('/') else '/data/'+name}))
# Image data and metadata compared byte-for-byte; source-size and edge limits included.
for name in ['ten-mib','ten-mib-plus','read-limit','read-limit-plus','padded.png','invalid.png','invalid.jpg','invalid.gif','invalid.webp','invalid.jpeg']:
    rs.append(call('read',{'file_path':'/data/'+name,'end_line':1}))
for w,h in [(1,1),(1999,1),(2000,1),(2001,1),(1,2001),(2000,2000),(2501,17)]:rs.append(call('read',{'file_path':f'/data/{w}x{h}.PNG'}))
for w,h in [(0,1),(1,0),(50000001,1),(10000,5001),(2147483647,2147483647)]:rs.append(call('read',{'file_path':f'/data/bad-{w}x{h}.png'}))
for ext in ['png','jpg','gif']:
    data=(ROOT/'recovery/images'/f'tiny.{ext}').read_bytes()
    rs.append(call('read',{'file_path':f'/data/mismatch-{ext}.webp'}))
    for n in [0,1,8,24,len(data)//2,len(data)-1]:rs.append(call('read',{'file_path':f'/data/truncated-{ext}-{n}.{ext}'}))
# Deterministic CRLF/BOM cross-product and exhaustive short LCS inputs.
texts=['','a','b','\n','a\n','a\nb','b\na','a\ra','a\r\n','a\r\nb\n','a\r\nb\r\nc\n','\ufeffa\n']
for i,(old,new) in enumerate(itertools.product(texts,repeat=2)):
    name=f'diff-{i}';rs.extend([call('write',{'file_path':name,'content':old}),call('write',{'file_path':name,'content':new})])
rng=random.Random(39021)
for i in range(150):
    old='\n'.join(rng.choice(['a','b','中','','\r']) for _ in range(rng.randrange(1,25)))
    new='\n'.join(rng.choice(['a','b','中','','\r']) for _ in range(rng.randrange(1,25)))
    rs.extend([call('write',{'file_path':f'random-{i}','content':old}),call('write',{'file_path':f'random-{i}','content':new})])
for n in [1999,2000,2001]:
    rs.extend([call('write',{'file_path':f'lcs-{n}','content':'a\n'*n}),call('write',{'file_path':f'lcs-{n}','content':'b\n'*n})])
for name in ['folder','loop','dangling','link','blocker/child','a\0b','x'*260]:
    rs.extend([call('write',{'file_path':name,'content':'new\n'}),call('edit',{'file_path':name,'old_string':'new','new_string':'edit'}),call('edit',{'file_path':name,'old_string':'','new_string':'create'})])
for old,new in [('', ''),('no','x'),('a','x'),('x',''),('\n','\r\n'),('中','utf')]:rs.append(call('edit',{'file_path':'lines','old_string':old,'new_string':new}))
rs.append(call('write',{'file_path':'preview','content':'changed'}))
group('boundaries',rs,fixture)

def permissions(p):
    p.chmod(0o777)
    (p/'locked').mkdir();(p/'sealed').mkdir()
    (p/'readonly').write_text('target')
    (p/'unreadable').write_text('target')
    (p/'sealed'/'target').write_text('target')
    (p/'locked'/'target').write_text('target')
    (p/'locked'/'writable').write_text('target');(p/'locked'/'writable').chmod(0o666)
    (p/'invalid.png').write_bytes(b'x')
    (p/'long').write_text('z'*60000);(p/'long').chmod(0o666)
rs=[]
for name in ['readonly','unreadable','sealed/target','locked/target','locked/writable','locked/new','invalid.png']:
    rs.extend([call('read',{'file_path':'/data/'+name}),call('edit',{'file_path':name,'old_string':'target','new_string':'edit'}),call('write',{'file_path':name,'content':'written'})])
rs.extend([call('write',{'file_path':'long','content':'x'*60000}),call('read',{'file_path':'/data/long'})])
group('permissions',rs,permissions,'mkdir -p /tmp/tool-outputs; chmod 555 /tmp/tool-outputs /data/locked; chmod 700 /data/sealed; chmod 444 /data/readonly /data/locked/target; chmod 000 /data/unreadable /data/invalid.png; exec su -s /bin/sh nobody -c "/tmp/server -workdir /data"')
rs=[call('write',{'file_path':'/limited/file','content':'z'*100000}),call('edit',{'file_path':'/limited/edit','old_string':'','new_string':'z'*100000}),call('write',{'file_path':'/data/persist','content':'中'*60000}),call('bash',{'command':'wc -c /limited/edit; ls -A /limited'})]
group('disk-full-and-persist-failure',rs,lambda p:None,'touch /tmp/tool-outputs; exec /tmp/server -workdir /data',('--tmpfs','/limited:rw,size=65536,mode=777'))

def mount_fixture(p):(p/'mounttarget').write_text('old\n')
group('rename-fallback',[call('write',{'file_path':'/mounted','content':'new\n'}),call('read',{'file_path':'/mounted'})],mount_fixture,'exec /tmp/server -workdir /data',('-v','{fixture}/mounttarget:/mounted'))
report={'binary':BINARY,'requests':sum(g['requests'] for g in groups),'groups':groups,'verified':verified,'failures':fail,'normalization':['preexisting normalize: tool output time paths / PID / registry order','atomic temp random Int63 only'],'checks':['full JSON including image base64','file contents / modes / symlinks','count and exit gates']}
(ROOT/'recovery/evidence/file-alignment-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print('Requests',report['requests'],'failures',len(fail))
for f in fail[:12]:print(json.dumps(f,ensure_ascii=False)[:2500])
sys.exit(bool(fail))
