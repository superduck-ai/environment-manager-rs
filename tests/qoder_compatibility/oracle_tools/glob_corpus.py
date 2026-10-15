import pathlib,json,random,shutil,os,sys,itertools
from probe import ROOT,run,call
from differential import canonical,normalize
base=ROOT/'recovery/glob-corpus';shutil.rmtree(base,ignore_errors=True);base.mkdir()
names=['a','b','abc','a1.txt','a2.txt','b1.txt','a01.txt','a-1.txt','a03.txt','x[1].txt','(a).txt','a|b.txt','a+b.txt','a@b.txt','a!b.txt','a?b.txt','ab.txt','aa.txt','b.txt','é.txt','中.txt','ABC.TXT',' .txt','a b.txt','a,b.txt','a{b}.txt']
for i,n in enumerate(names):
 for prefix in ['', 'dir/', 'dir/sub/']:
  p=base/(prefix+n);p.parent.mkdir(parents=True,exist_ok=True);p.write_text('x\n');os.utime(p,(1700000000+i,1700000000+i))
patterns=['!','!!','!!!','!()','@()','+()','?()','*()','@(a|b)','@(a|@(b|abc))','!(!(a)).txt','+(a|b).txt','*(a|b).txt','?(a|b).txt','!(a|b).txt','[abc].txt','[].txt','[]a].txt','[-a].txt','[a-].txt','[\\]].txt','[!].txt','[^a].txt','[[:ascii:]].txt','[[:word:]].txt','[[:space:]].txt','[[:bogus:]].txt','[é-中].txt','[中-é].txt','{a,b}.txt','{a,{b,abc}}.txt','{a}.txt','{,a}.txt','a{01..03}.txt','a{3..1}.txt','a{-1..1}.txt','a{1..3..-1}.txt','a{a..c}.txt','a{1..x}.txt','a{1..3..0}.txt','a{1..3..x}.txt','a{1..3..2..1}.txt',r'a\?b.txt',r'x\[1\].txt',r'a\{b\}.txt',r'a\,b.txt','**','**/*.txt','dir/**/*.txt','dir/**','**/a','dir/../*.txt','./dir/*','/data/dir/*','/data/dir/../*.txt','/tmp','/tmp/server','/data','/data/','a/**b','a***','dir/**/**/a','dir/**a','a)','a(','a|b.txt','@(a','a\\','\\']
rng=random.Random(91);tokens=['a','b','c','*','?','[ab]','[!a]','@(a|b)','+(a|b)','!(a|b)','?(a|b)','*(a|b)','{a,b}']
for _ in range(200):
 p=''.join(rng.choice(tokens) for _ in range(rng.randrange(1,5)))+rng.choice(['','.txt'])
 p=rng.choice(['','dir/','**/','!','!!'])+p;patterns.append(p)
rs=[call('glob',{'pattern':p},i+1) for i,p in enumerate(patterns)]
rs.extend(call('read',{'file_path':'/data/'+n},len(rs)+i+1) for i,n in enumerate(['a4.txt','abx.txt','xyz.txt','abcx','x','z','ax.txt','ABC.txt','a100.txt','中x.txt']))
one,e1,c1=run(rs,fixture=base);two,e2,c2=run(rs,binary='recovery/rebuilt-mcp-server',fixture=base)
fail=[{'request':rs[i],'original':x,'rebuilt':y} for i,(x,y) in enumerate(itertools.zip_longest(one,two)) if canonical(normalize(x))!=canonical(normalize(y))]
if len(one)!=len(rs) or len(two)!=len(rs):fail.append({'response_counts':[len(one),len(two)],'expected':len(rs)})
if c1 or c2:fail.append({'exit_codes':[c1,c2]})
report={'cases':len(rs),'responses':[len(one),len(two)],'failures':fail};(ROOT/'recovery/evidence/glob-corpus-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print('Cases',len(rs),'failures',len(fail))
for f in fail:print(json.dumps(f,ensure_ascii=False)[:4500])
sys.exit(bool(fail))
