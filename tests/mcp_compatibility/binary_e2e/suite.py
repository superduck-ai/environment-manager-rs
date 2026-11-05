"""从 MCP 客户端功能层面对可执行文件执行相同的端到端流程。"""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import queue
import re
import shutil
import socket
import subprocess
import sys
import threading
import time

DATA = Path('/data')
PORT = 32191
BINARY = sys.argv[1]
REPORT = Path(sys.argv[2])
TOOLS = {'bash', 'read', 'write', 'edit', 'grep', 'glob', 'run_in_terminal',
         'file_read', 'file_write', 'file_edit', 'grep_run', 'glob_run'}
ENV = {'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
       'HOME': '/root', 'LANG': 'C', 'LC_ALL': 'C', 'WORK_DIR': '/data', 'INHERITED': 'parent'}


def wait_for(predicate, timeout=8):
    """以实际完成条件等待，超时视为失败，而非比较毫秒级调度差异。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.02)
    raise AssertionError('condition not satisfied before deadline')


def fixture():
    """为每种传输创建相同文件、模式、符号链接和确定的修改时间。"""
    shutil.rmtree(DATA, ignore_errors=True)
    DATA.mkdir(mode=0o755)
    files = {'lines.txt': b'alpha\nbeta\ngamma\n', 'empty.txt': b'',
             'unicode.txt': '中文🙂\nsecond\n'.encode(), 'invalid.txt': b'a\xffb\n',
             'binary.bin': b'a\0b', 'crlf.txt': b'\xef\xbb\xbfalpha\r\nbeta\r\n',
             'repeat.txt': b'same same\n', 'search.txt': b'before\nMatch one\nmiddle\nmatch two\nafter\n',
             'multiline.txt': b'struct {\n field int\n}\n', 'readonly.txt': b'old\n',
             'tree/a.rs': b'alpha\n', 'tree/b.txt': b'beta\n', 'tree/sub/c.rs': b'alpha\n',
             'tree/.hidden.txt': b'hidden\n', 'tree/.gitignore': b'ignored.txt\n',
             'tree/ignored.txt': b'ignored\n', 'tree/a1.txt': b'alpha\n',
             'tree/a2.txt': b'alpha\n', 'tree/x[1].txt': b'brackets\n'}
    for index, (name, content) in enumerate(files.items()):
        path = DATA / name
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        path.write_bytes(content)
        path.chmod(0o640 if name == 'readonly.txt' else 0o644)
        os.utime(path, (1700000000 + index, 1700000000 + index))
    (DATA / 'link.txt').symlink_to('lines.txt')
    (DATA / 'broken.txt').symlink_to('missing-target.txt')
    images = DATA / 'images'
    images.mkdir()
    for path in sorted(Path('/image-fixtures').iterdir()):
        if '.expected.' not in path.name:
            shutil.copyfile(path, images / path.name)
    for path in sorted(Path('/suite/fixtures').glob('*.webp')):
        shutil.copyfile(path, images / path.name)
    (images / 'bad.png').write_bytes(b'not an image')
    (images / 'unsupported.bmp').write_bytes(b'BMfake')
    (DATA / 'large.txt').write_bytes(b'long content\n' * 6000)


def snapshot():
    """核对真实文件字节、权限及链接；不比较系统时钟和 inode。"""
    result = {}
    for path in sorted(DATA.rglob('*')):
        name = str(path.relative_to(DATA))
        if path.is_symlink():
            result[name] = {'link': os.readlink(path)}
        elif path.is_file():
            result[name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                            'size': path.stat().st_size, 'mode': oct(path.stat().st_mode & 0o7777)}
        elif path.is_dir():
            result[name] = {'directory': True, 'mode': oct(path.stat().st_mode & 0o7777)}
    return result


class Client:
    """使用真实 stdio 或 HTTP 请求，保持服务运行以验证有状态工作流。"""
    def __init__(self, transport, extra=(), env=None):
        self.transport = transport
        self.counter = 0
        self.requests = 0
        self.errors = open('/tmp/server-stderr.log', 'wb')
        args = [BINARY, '-workdir', '/data', '-transport', transport, *extra]
        if transport == 'http':
            args += ['-addr', f'127.0.0.1:{PORT}']
        self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.errors, env=env or ENV)
        self.lines = queue.Queue()
        if transport == 'stdio':
            self.reader = threading.Thread(target=self._read, daemon=True)
            self.reader.start()
        else:
            def ready():
                if self.proc.poll() is not None:
                    raise AssertionError('HTTP server exited during startup')
                try:
                    with socket.create_connection(('127.0.0.1', PORT), timeout=.1):
                        return True
                except OSError:
                    return False
            wait_for(ready)

    def _read(self):
        """独立消费响应，保留超时和意外 EOF 为显式失败。"""
        for line in self.proc.stdout:
            self.lines.put(line)
        self.lines.put(None)

    def http(self, method, path='/mcp', body=None):
        """记录功能相关响应头，不比较 Date、分块和 Content-Length。"""
        connection = http.client.HTTPConnection('127.0.0.1', PORT, timeout=10)
        try:
            connection.request(method, path, body=body)
            response = connection.getresponse()
            data = response.read()
            headers = {k.lower(): v for k, v in response.getheaders()
                       if k.lower() in ('content-type', 'x-content-type-options', 'allow', 'location')}
            try:
                value = json.loads(data)
            except (ValueError, UnicodeDecodeError):
                value = data.decode(errors='replace')
            return {'status': response.status, 'headers': headers, 'body': value}
        finally:
            connection.close()

    def exchange(self, request, notification=False):
        """通知以后续 ping 作响应栅栏，验证确实没有多余响应。"""
        self.requests += 1
        payload = request if isinstance(request, str) else json.dumps(request, ensure_ascii=False)
        if self.transport == 'http':
            return self.http('POST', body=payload.encode())
        self.proc.stdin.write((payload + '\n').encode())
        self.proc.stdin.flush()
        if notification:
            fence = {'jsonrpc': '2.0', 'id': 'notification-fence', 'method': 'ping'}
            self.proc.stdin.write((json.dumps(fence) + '\n').encode())
            self.proc.stdin.flush()
            self.requests += 1
        line = self.lines.get(timeout=10)
        if line is None:
            raise AssertionError('unexpected stdio EOF')
        response = json.loads(line)
        if notification:
            assert response == {'jsonrpc': '2.0', 'id': 'notification-fence', 'result': {}}, response
            return None
        return response

    def rpc(self, method, params=None):
        """为功能请求分配可追踪的递增 ID。"""
        self.counter += 1
        request = {'jsonrpc': '2.0', 'id': self.counter, 'method': method}
        if params is not None:
            request['params'] = params
        return request

    def stop(self):
        """回收服务器，防止后续用例复用遗留监听端口。"""
        self.proc.terminate()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.errors.close()


def body(response, transport):
    """提取 JSON-RPC 响应，不改写其内容。"""
    return response['body'] if transport == 'http' else response


def text(response, transport):
    """读取工具文本内容用于额外行为断言。"""
    return '\n'.join(block.get('text', '') for block in body(response, transport).get('result', {}).get('content', []))


def normalized(value):
    """仅忽略 Go map 枚举顺序，其他 JSON 数组和用户文本原样比较。"""
    if isinstance(value, dict):
        result = {key: normalized(item) for key, item in value.items()}
        if isinstance(result.get('tools'), list):
            result['tools'].sort(key=lambda tool: tool['name'])
        return result
    if isinstance(value, list):
        return [normalized(item) for item in value]
    return value


class Suite:
    """收集完整轨迹与独立断言，失败时也输出可定位报告。"""
    def __init__(self):
        self.steps = []
        self.failures = []
        self.requests = 0
        self.timings = {}

    def check(self, name, condition, detail):
        """差分之外检查功能预期，避免两端同样失败时误判通过。"""
        if not condition:
            self.failures.append({'name': name, 'reason': detail})

    def record(self, client, name, request, *, notification=False, files=False, artifact=False):
        """保留请求、响应和副作用；动态产物路径只在指定用例处理。"""
        started = time.monotonic()
        response = client.exchange(request, notification=notification)
        step = {'name': f'{client.transport}/{name}', 'request': request,
                'response': normalized(response)}
        self.timings[step['name']] = round((time.monotonic() - started) * 1000)
        if notification and client.transport == 'http':
            self.check(step['name'], response['status'] == 202 and response['body'] == '', 'HTTP notification must return empty 202')
        if files:
            step['files'] = snapshot()
        if artifact:
            message = text(response, client.transport)
            paths = list(dict.fromkeys(re.findall(r'/tmp/tool-outputs/[A-Za-z0-9_.-]+', message)))
            outputs = []
            for path in paths:
                file = Path(path)
                self.check(step['name'], file.is_file(), f'missing output artifact: {path}')
                if file.is_file():
                    content = file.read_bytes()
                    outputs.append({'sha256': hashlib.sha256(content).hexdigest(), 'size': len(content)})
            # 仅作用于工具生成的 bash/大输出包装；调用者文本不做全局替换。
            encoded = json.dumps(step['response'], ensure_ascii=False)
            for index, path in enumerate(paths):
                encoded = encoded.replace(path, f'<output-file-{index}>')
            if message.startswith('[backgrounded]'):
                encoded = re.sub(r'pid: \d+', 'pid: <pid>', encoded)
            step['response'] = json.loads(encoded)
            step['artifacts'] = outputs
        self.steps.append(step)
        return response

    def tool(self, client, name, tool, arguments, **options):
        """所有工具均经过公开 MCP tools/call 路由。"""
        return self.record(client, name, client.rpc('tools/call', {'name': tool, 'arguments': arguments}), **options)

    def functional(self, transport):
        """按真实客户端流程验证协议、文件、搜索、别名和进程管理。"""
        fixture()
        client = Client(transport)
        try:
            self.protocol(client)
            self.files(client)
            self.search(client)
            self.commands(client)
            self.validation(client)
            if transport == 'http':
                self.http_routes(client)
                self.http_concurrency_and_cancel(client)
        finally:
            self.requests += client.requests
            client.stop()

    def protocol(self, client):
        """初始化、发现、通知、ID 和非法请求的协议行为。"""
        r = self.record(client, 'initialize', client.rpc('initialize', {'protocolVersion': '2024-11-05', 'capabilities': {}, 'clientInfo': {'name': 'functional-e2e', 'version': '1'}}))
        self.check(f'{client.transport}/initialize', body(r, client.transport).get('result', {}).get('capabilities') == {'tools': {}}, 'missing tool capability')
        self.record(client, 'initialized-notification', {'jsonrpc': '2.0', 'method': 'notifications/initialized'}, notification=True)
        self.record(client, 'cancel-notification', {'jsonrpc': '2.0', 'method': 'notifications/cancelled', 'params': {'requestId': 'absent', 'reason': 'client cancelled'}}, notification=True)
        self.record(client, 'ping-after-notification', client.rpc('ping'))
        r = self.record(client, 'tool-discovery', client.rpc('tools/list'))
        definitions = body(r, client.transport)['result']['tools']
        self.check(f'{client.transport}/tool-discovery', {d['name'] for d in definitions} == TOOLS, 'expected six tools and six aliases')
        for method in ('resources/list', 'prompts/list', 'tools/unknown'):
            self.record(client, 'unsupported-' + method, client.rpc(method))
        for index, ident in enumerate([0, False, '', '中文🙂', 1.25, 9007199254740992]):
            self.record(client, f'id-{index}', {'jsonrpc': '2.0', 'id': ident, 'method': 'ping'})
        for index, request in enumerate(['{', '[]', 'null', '{}', '{"jsonrpc":"1.0","id":9,"method":"ping"}', '{"jsonrpc":"2.0","id":9,"method":"unknown"}', '{"jsonrpc":"2.0","id":9,"method":"tools/call"}', '{"jsonrpc":"2.0","id":9,"method":"tools/call","params":[]}', '{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"unknown","arguments":{}}}']):
            self.record(client, f'protocol-error-{index}', request)
        for index, request in enumerate([{'jsonrpc': '2.0', 'method': 'ping'}, {'jsonrpc': '2.0', 'id': None, 'method': 'ping'}, {'jsonrpc': '2.0', 'method': 'tools/call', 'params': {'name': 'write', 'arguments': {'file_path': '/data/notification-must-not-write', 'content': 'unexpected'}}}]):
            self.record(client, f'notification-{index}', request, notification=True, files=True)
        self.check(f'{client.transport}/notification-side-effect', not (DATA / 'notification-must-not-write').exists(), 'notification executed write')
        for index, fields in enumerate(['"arguments":{"file_path":"/data/lines.txt"},"arguments":{"start_line":1}', '"arguments":{"file_path":"/data/missing"},"ARGUMENTS":{"file_path":"/data/lines.txt"}', '"arguments":{"file_path":"/data/lines.txt"},"arguments":null,"arguments":{"start_line":0}', '"arguments":null,"arguments":{"file_path":"/data/lines.txt"}', '"arguments":{"file_path":"/data/lines.txt"},"arguments":null']):
            self.record(client, f'repeated-arguments-{index}', '{"jsonrpc":"2.0","id":500,"method":"tools/call","params":{"name":"read",' + fields + '}}')

    def files(self, client):
        """文件生命周期、分页、编码、权限、链接、错误和真实图片字节。"""
        for index, arguments in enumerate([{'file_path': '/data/lines.txt'}, {'file_path': '/data/lines.txt', 'start_line': 1, 'end_line': 2}, {'file_path': '/data/lines.txt', 'start_line': -2, 'end_line': 99}, {'file_path': '/data/lines.txt', 'start_line': 9}, {'file_path': '/data/lines.txt', 'start_line': 2, 'end_line': 1}, *[{'file_path': '/data/' + p} for p in ['empty.txt', 'unicode.txt', 'invalid.txt', 'binary.bin', 'crlf.txt', 'link.txt', 'broken.txt', 'missing.txt', 'tree']], {'file_path': '/dev/zero'}, {'file_path': '//server/share'}]):
            self.tool(client, f'read-{index}', 'read', arguments)
        r = self.tool(client, 'write-create', 'write', {'file_path': '/data/work/note.txt', 'content': 'first\nsecond\n'}, files=True)
        self.check(f'{client.transport}/write-create', not body(r, client.transport)['result'].get('isError', False) and (DATA / 'work/note.txt').read_bytes() == b'first\nsecond\n', 'write did not create expected content')
        self.tool(client, 'read-created', 'read', {'file_path': '/data/work/note.txt'})
        self.tool(client, 'edit-created', 'edit', {'file_path': '/data/work/note.txt', 'old_string': 'second', 'new_string': 'changed🙂'}, files=True)
        self.tool(client, 'alias-read-edited', 'file_read', {'file_path': '/data/work/note.txt', 'startLine': 1, 'endLine': 2})
        self.tool(client, 'alias-overwrite', 'file_write', {'file_path': '/data/work/note.txt', 'content': 'replacement\n'}, files=True)
        self.tool(client, 'alias-edit', 'file_edit', {'file_path': '/data/work/note.txt', 'old_string': 'replacement', 'new_string': 'final'}, files=True)
        self.tool(client, 'read-final', 'read', {'file_path': '/data/work/note.txt'})
        self.check(f'{client.transport}/edit-final', (DATA / 'work/note.txt').read_bytes() == b'final\n', 'workflow final bytes differ')
        for index, (tool, args) in enumerate([
            ('write', {'file_path': '/data/crlf.txt', 'content': 'new\nvalue\n'}),
            ('edit', {'file_path': '/data/crlf.txt', 'old_string': 'value', 'new_string': 'updated'}),
            ('write', {'file_path': '/data/readonly.txt', 'content': 'changed\n'}),
            ('write', {'file_path': '/data/link.txt', 'content': 'via link\n'}),
            ('write', {'file_path': '/data/broken.txt', 'content': 'repair\n'}),
            ('edit', {'file_path': '/data/edit-new.txt', 'old_string': '', 'new_string': 'created by edit\n'}),
            ('edit', {'file_path': '/data/repeat.txt', 'old_string': 'same', 'new_string': 'changed'}),
            ('edit', {'file_path': '/data/repeat.txt', 'old_string': 'absent', 'new_string': 'changed'}),
            ('edit', {'file_path': '/data/not-existing.txt', 'old_string': 'x', 'new_string': 'y'}),
            ('write', {'file_path': '/data/tree', 'content': 'no'}),
            ('edit', {'file_path': '/data/tree', 'old_string': '', 'new_string': 'no'}),
            ('write', {'file_path': '/data/work/empty.txt', 'content': ''}),
            ('write', {'file_path': '/data/work/escape.txt', 'content': '<tag>&\u2028\u2029中文🙂'}),
        ]):
            self.tool(client, f'file-mutation-{index}', tool, args, files=True)
        self.check(f'{client.transport}/preserve-format', (DATA / 'crlf.txt').read_bytes() == b'\xef\xbb\xbfnew\r\nupdated\r\n', 'BOM/CRLF lost')
        self.check(f'{client.transport}/preserve-mode', (DATA / 'readonly.txt').stat().st_mode & 0o777 == 0o640, 'file mode lost')
        self.check(f'{client.transport}/preserve-link', (DATA / 'link.txt').is_symlink(), 'symlink replaced')
        self.tool(client, 'read-large-truncated', 'read', {'file_path': '/data/large.txt'}, artifact=True)
        self.tool(client, 'read-large-page', 'read', {'file_path': '/data/large.txt', 'start_line': 5990, 'end_line': 6000})
        for index, path in enumerate(sorted((DATA / 'images').iterdir())):
            r = self.tool(client, f'image-{index}-{path.name}', 'read', {'file_path': str(path)})
            if path.name not in ('bad.png', 'unsupported.bmp'):
                images = [block for block in body(r, client.transport)['result']['content'] if block['type'] == 'image']
                self.check(f'{client.transport}/image-{index}', len(images) == 1 and bool(base64.b64decode(images[0]['data'], validate=True)), 'image payload missing or invalid base64')
        for path in sorted(Path('/suite/fixtures').glob('*.webp')):
            padded = DATA / 'images' / ('large-bytes-' + path.name)
            padded.write_bytes(path.read_bytes() + bytes(4_000_000))
            r = self.tool(client, 'image-reencoded-' + path.name, 'read', {'file_path': str(padded)})
            self.check(f'{client.transport}/image-reencoded-{path.name}', not body(r, client.transport)['result'].get('isError', False), 'large WebP failed to decode/re-encode')
            padded.unlink()
        self.tool(client, 'delete-via-bash', 'bash', {'command': 'rm /data/work/note.txt'}, files=True)
        self.tool(client, 'read-after-delete', 'read', {'file_path': '/data/work/note.txt'})

    def search(self, client):
        """真实 ripgrep 和 glob 的模式、过滤、分页及组合操作。"""
        options = [{}, {'output_mode': 'content'}, {'output_mode': 'count'}, {'output_mode': 'files_with_matches'}, {'output_mode': 'content', '-i': True}, {'output_mode': 'content', '-n': False}, {'output_mode': 'content', 'context': 1}, {'output_mode': 'content', '-B': 1, '-A': 1}, {'output_mode': 'content', 'head_limit': 1}, {'output_mode': 'content', 'offset': 1, 'head_limit': 1}, {'output_mode': 'content', 'head_limit': 0}, {'output_mode': 'bad'}]
        for index, extra in enumerate(options):
            self.tool(client, f'grep-option-{index}', 'grep', {'path': '/data/search.txt', 'pattern': 'match', **extra})
        for index, pattern in enumerate(['not-found', '[', '-literal', '^']):
            self.tool(client, f'grep-pattern-{index}', 'grep', {'path': '/data/search.txt', 'pattern': pattern, 'output_mode': 'content'})
        self.tool(client, 'grep-multiline', 'grep', {'path': '/data/multiline.txt', 'pattern': 'struct.*field', 'multiline': True, 'output_mode': 'content'})
        for index, extra in enumerate([{'include': '*.rs'}, {'type': 'rust'}, {'include': '*.txt'}, {'include': '*.rs,*.txt'}, {'type': 'unknown'}]):
            self.tool(client, f'grep-filter-{index}', 'grep', {'path': '/data/tree', 'pattern': 'alpha', **extra})
        self.tool(client, 'alias-grep-filter', 'grep_run', {'path': '/data/tree', 'pattern': 'alpha', 'glob': '*.rs'})
        self.tool(client, 'grep-missing-directory', 'grep', {'path': '/data/missing-tree', 'pattern': 'alpha'})
        for index, pattern in enumerate(['*', '**/*.rs', '**/*.txt', 'sub/*', 'a[12].txt', 'a{1,2}.txt', 'a{1..2}.txt', '@(a1|a2).txt', '!(a1).txt', r'x\[1\].txt', '.hidden*', '*.notfound', '[', '[z-a]']):
            self.tool(client, f'glob-pattern-{index}', 'glob', {'path': '/data/tree', 'pattern': pattern})
        self.tool(client, 'alias-glob', 'glob_run', {'path': '/data/tree', 'pattern': '**/*.rs'})
        self.tool(client, 'glob-missing-directory', 'glob', {'path': '/data/missing-tree', 'pattern': '*'})
        self.tool(client, 'search-workflow-write', 'write', {'file_path': '/data/tree/workflow.rs', 'content': 'fn target() {}\n'}, files=True)
        self.tool(client, 'search-workflow-discover', 'glob', {'path': '/data/tree', 'pattern': '*workflow*'})
        self.tool(client, 'search-workflow-match', 'grep', {'path': '/data/tree/workflow.rs', 'pattern': 'target', 'output_mode': 'content'})
        self.tool(client, 'search-workflow-edit', 'edit', {'file_path': '/data/tree/workflow.rs', 'old_string': 'target', 'new_string': 'renamed'}, files=True)
        self.tool(client, 'search-workflow-old-gone', 'grep', {'path': '/data/tree/workflow.rs', 'pattern': 'target'})
        self.tool(client, 'search-workflow-new-present', 'grep', {'path': '/data/tree/workflow.rs', 'pattern': 'renamed', 'output_mode': 'content'})

    def commands(self, client):
        """shell 输出、环境、工作目录、非交互限制、超时和后台产物。"""
        cases = [
            ('stdout', {'command': 'printf hello'}),
            ('stderr-and-exit', {'command': 'printf out; printf err >&2; exit 7'}),
            ('empty-output', {'command': 'true'}),
            ('unicode', {'command': "printf '中文🙂<&>'"}),
            ('cwd', {'command': 'pwd'}),
            ('relative-cwd', {'command': 'pwd', 'work_dir': 'tree'}),
            ('absolute-cwd', {'command': 'pwd', 'work_dir': '/data/tree'}),
            ('missing-cwd', {'command': 'pwd', 'work_dir': 'absent'}),
            ('env-override', {'command': 'printf "%s:%s:%s" "$INHERITED" "$CUSTOM" "$CI"', 'env': {'CUSTOM': 'value', 'CI': 'override'}}),
            ('env-child', {'command': "bash -c 'printf %s \"$CUSTOM\"'", 'env': {'CUSTOM': 'child'}}),
            ('env-invalid-type', {'command': 'true', 'env': {'BAD': 3}}),
            ('invalid-timeout-policy', {'command': 'true', 'on_timeout': 'unknown'}),
            ('interactive-editor', {'command': 'vim file'}),
            ('interactive-pager', {'command': 'less file'}),
            ('quoted-editor-name', {'command': "printf 'vim file; less file'"}),
            ('literal-generated-looking-text', {'command': "printf '/tmp/tool-outputs/bash_1234.log\\npid: 987'"}),
            ('stdin-is-null', {'command': 'cat'}),
            ('tty-unavailable', {'command': 'if test -t 0; then echo tty; else echo no-tty; fi'}),
            ('alias-timeout-map', {'command': 'printf alias', 'timeout': 0, 'wait_time': 1, 'session_id': 'ignored'}),
        ]
        for name, args in cases:
            self.tool(client, 'bash-' + name, 'run_in_terminal' if name.startswith('alias') else 'bash', args)
        r = self.tool(client, 'bash-large-output', 'bash', {'command': "printf '%040000d' 0"}, artifact=True)
        self.check(f'{client.transport}/bash-large-output', len(text(r, client.transport)) < 40000, 'large output not bounded')
        killed = DATA / 'timeout-leak.txt'
        r = self.tool(client, 'bash-timeout-group', 'bash', {'command': '(sleep 3; printf leaked > /data/timeout-leak.txt) & wait', 'timeout': 1}, files=True)
        self.check(f'{client.transport}/bash-timeout-group', 'timed out' in text(r, client.transport), 'timeout not reported')
        time.sleep(2.2)
        self.steps.append({'name': f'{client.transport}/timeout-side-effect', 'files': snapshot()})
        self.check(f'{client.transport}/timeout-side-effect', not killed.exists(), 'descendant survived timeout')
        r = self.tool(client, 'bash-background', 'bash', {'command': 'printf ready; sleep 2; printf done; printf complete > /data/background-done.txt', 'timeout': 1, 'on_timeout': 'background'}, artifact=True)
        self.check(f'{client.transport}/bash-background', text(r, client.transport).startswith('[backgrounded]'), 'background handle missing')
        path_match = re.search(r'output_file: (\S+)', text(r, client.transport))
        wait_for(lambda: (DATA / 'background-done.txt').exists())
        self.check(f'{client.transport}/background-complete', (DATA / 'background-done.txt').read_bytes() == b'complete', 'background did not finish')
        if path_match:
            output = Path(path_match.group(1))
            wait_for(lambda: output.is_file() and output.read_bytes().endswith(b'\n[exit code: 0]\n'))
            # MCP 客户端使用返回的 output_file 读取最终日志，记录归一化后的请求路径。
            request = client.rpc('tools/call', {'name': 'read', 'arguments': {'file_path': str(output)}})
            response = client.exchange(request)
            self.steps.append({'name': f'{client.transport}/background-read-output', 'request': {'output_file': '<returned handle>'}, 'response': normalized(response), 'output_sha256': hashlib.sha256(output.read_bytes()).hexdigest(), 'files': snapshot()})
            self.check(f'{client.transport}/background-read-output', 'readydone' in text(response, client.transport), 'final output unavailable through MCP read')
        else:
            self.check(f'{client.transport}/background-read-output', False, 'output_file missing')
        self.record(client, 'ping-after-process-tests', client.rpc('ping'))

    def validation(self, client):
        """全部十二个公开工具的必填参数和类型错误经协议返回。"""
        for tool in sorted(TOOLS):
            for index, arguments in enumerate([{}, None, [], {'file_path': None, 'command': None, 'pattern': None}, {'file_path': 17, 'command': 17, 'pattern': 17}]):
                self.tool(client, f'validation-{tool}-{index}', tool, arguments, files=True)

    def http_routes(self, client):
        """HTTP 状态、通知、方法限制、路径重定向和坏请求。"""
        for index, (method, path, payload) in enumerate([('GET', '/mcp', None), ('HEAD', '/mcp', None), ('PUT', '/mcp', b'{}'), ('DELETE', '/mcp', None), ('GET', '/', None), ('POST', '/mcp/', b'{}'), ('GET', '/x/../mcp', None), ('POST', '/mcp?client=e2e', b'{"jsonrpc":"2.0","id":1,"method":"ping"}'), ('POST', '/mcp', b''), ('POST', '/mcp', b'{} {}')]):
            self.steps.append({'name': f'http/route-{index}', 'request': [method, path, payload.decode() if payload is not None else None], 'response': client.http(method, path, payload)})

    def http_concurrency_and_cancel(self, client):
        """并发请求及客户端断开必须真正终止 shell 的进程组。"""
        with ThreadPoolExecutor(max_workers=1) as pool:
            task = pool.submit(client.exchange, client.rpc('tools/call', {'name': 'bash', 'arguments': {'command': 'printf started > /data/concurrent-start; sleep 1; printf finished'}}))
            wait_for(lambda: (DATA / 'concurrent-start').exists())
            self.record(client, 'concurrent-ping', client.rpc('ping'))
            self.check('http/concurrent-ping', not task.done(), 'ping blocked behind shell execution')
            response = task.result(timeout=5)
            self.steps.append({'name': 'http/concurrent-shell', 'response': normalized(response)})
        request = json.dumps(client.rpc('tools/call', {'name': 'bash', 'arguments': {'command': 'printf started > /data/cancel-start; (printf started > /data/cancel-child-start; sleep 3; printf leaked > /data/cancel-leak) & wait', 'timeout': 10}})).encode()
        sock = socket.create_connection(('127.0.0.1', PORT), timeout=3)
        sock.sendall(f'POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {len(request)}\r\n\r\n'.encode() + request)
        client.requests += 1
        wait_for(lambda: (DATA / 'cancel-child-start').exists())
        sock.close()
        time.sleep(3.3)
        self.check('http/cancel-side-effect', not (DATA / 'cancel-leak').exists(), 'descendant survived client disconnect')
        self.steps.append({'name': 'http/cancel-side-effect', 'files': snapshot()})
        self.record(client, 'ping-after-cancel', client.rpc('ping'))

    def environments(self, transport):
        """在真实 Unix socket 上注入分块/尾部空白/错误环境快照。"""
        big = json.dumps({'env': {'GOOD': 'runtime', 'PADDING': 'x' * 12000}}).encode()
        prefix = b'{"env":{"GOOD":"runtime"},"padding":"'
        suffix = b'"}'
        at_limit = prefix + b'x' * ((1 << 20) - len(prefix) - len(suffix)) + suffix
        unicode_snapshot = json.dumps({'env': {'GOOD': '中文🙂'}}, ensure_ascii=False).encode()
        split = unicode_snapshot.index('中'.encode()) + 1
        cases = [
            ('small', [b'{"env":{"GOOD":"runtime","CI":"runtime"}}'], True),
            ('large', [big], True),
            ('large-newline', [big + b'\n'], True),
            ('large-spaces', [big + b' \t\r\n'], True),
            ('chunked-large', [big[:4096], big[4096:8192], big[8192:] + b'\n'], True),
            ('chunked-unicode', [unicode_snapshot[:split], unicode_snapshot[split:] + b'\n'], True),
            ('null-map', [b'{"env":null}'], True),
            ('no-env', [b'{}'], True),
            ('null-snapshot', [b'null'], True),
            ('null-snapshot-newline', [b'null\n'], True),
            ('null-snapshot-eof', [b'null'], False),
            ('bare-boolean', [b'true'], True),
            ('boolean-newline', [b'true\n'], True),
            ('bare-number', [b'1'], True),
            ('number-newline', [b'1\n'], True),
            ('bare-string', [b'"text"'], True),
            ('string-newline', [b'"text"\n'], True),
            ('invalid-utf8', [b'{"env":{"GOOD":"a\xffb"}}'], True),
            ('orphan-surrogate', [br'{"env":{"GOOD":"\ud800"}}'], True),
            ('chunked-null', [b'nu', b'll', b'\n'], True),
            ('malformed-open', [b'{"env":!'], True),
            ('duplicate-env', [b'{"env":{"GOOD":"first","OTHER":"kept"},"env":{"GOOD":"last"}}'], True),
            ('invalid-value', [b'{"env":{"GOOD":123}}'], False),
            ('malformed-eof', [b'{'], False),
            ('timeout', [], True),
            ('first-value-only', [b'{"env":{"GOOD":"first"}} {"env":{"GOOD":"second"}}'], True),
            ('half-limit', [prefix + b'x' * ((1 << 19) - len(prefix) - len(suffix)) + suffix], True),
            ('at-limit', [at_limit], True),
            ('oversized', [b'{"env":{"GOOD":"' + b'x' * (1 << 20) + b'"}}'], False),
        ]
        for name, chunks, keep_open in cases:
            fixture()
            path = Path('/tmp/environment-e2e.sock')
            path.unlink(missing_ok=True)
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(str(path))
            listener.listen(1)
            listener.settimeout(5)
            release = threading.Event()
            def serve():
                try:
                    conn, _ = listener.accept()
                    with conn:
                        for chunk in chunks:
                            try:
                                conn.sendall(chunk)
                            except (BrokenPipeError, ConnectionResetError):
                                break
                            if len(chunks) > 1:
                                time.sleep(.02)
                        if keep_open:
                            release.wait(3)
                except (OSError, TimeoutError):
                    pass
                finally:
                    listener.close()
            thread = threading.Thread(target=serve)
            thread.start()
            client = Client(transport, ['-env-socket', str(path)])
            try:
                r = self.tool(client, 'environment-' + name, 'bash', {'command': 'printf "%s|%s|%s|%s" "$GOOD" "$OTHER" "$CI" "$CUSTOM"', 'env': {'CUSTOM': 'call'}})
                self.steps[-1]['environment_input'] = {'keep_open': keep_open,
                    'chunks': [{'size': len(chunk), 'sha256': hashlib.sha256(chunk).hexdigest()} for chunk in chunks]}
                if name in ('small', 'large', 'large-newline', 'large-spaces', 'chunked-large', 'chunked-unicode', 'first-value-only', 'duplicate-env', 'half-limit', 'at-limit'):
                    self.check(f'{transport}/environment-{name}', not body(r, transport)['result'].get('isError', False), 'complete snapshot rejected')
            finally:
                self.requests += client.requests
                client.stop()
                release.set()
                thread.join(5)
                path.unlink(missing_ok=True)
        fixture()
        client = Client(transport, ['-env-socket', '/tmp/missing-environment-e2e.sock'])
        try:
            self.tool(client, 'environment-missing-socket', 'bash', {'command': 'printf unexpected'})
        finally:
            self.requests += client.requests
            client.stop()
        # WORK_DIR 覆盖命令行默认值，工具的调用级 work_dir 仍然优先。
        client = Client(transport, ['-workdir', '/tmp'])
        try:
            r = self.tool(client, 'workdir-env-overrides-cli', 'bash', {'command': 'pwd'})
            self.check(f'{transport}/workdir-env-overrides-cli', text(r, transport).strip() == '/data', 'WORK_DIR precedence differs')
            self.tool(client, 'workdir-call-overrides-env', 'bash', {'command': 'pwd', 'work_dir': 'tree'})
        finally:
            self.requests += client.requests
            client.stop()

    def stdio_framing(self):
        """多行、无末尾换行、空行和通知不破坏后续请求边界。"""
        for name, payload in [('unterminated-line', b'{"jsonrpc":"2.0","id":1,"method":"ping"}'), ('blank-and-two-lines', b'\n{"jsonrpc":"2.0","id":1,"method":"ping"}\n{"jsonrpc":"2.0","id":2,"method":"ping"}\n')]:
            out = subprocess.run([BINARY, '-workdir', '/data'], input=payload, capture_output=True, env=ENV, timeout=10)
            responses = [json.loads(line) for line in out.stdout.splitlines()]
            self.steps.append({'name': 'stdio/framing-' + name, 'responses': responses, 'exit_code': out.returncode})
            self.requests += len(payload.splitlines())
            if name == 'unterminated-line':
                self.check('stdio/framing-' + name, not responses and out.returncode == 0, 'unterminated line should be ignored')

    def run(self):
        """输出执行环境、完整可重放步骤和失败断言。"""
        fatal = None
        try:
            for transport in ('stdio', 'http'):
                self.functional(transport)
                self.environments(transport)
            self.stdio_framing()
        except Exception as error:
            fatal = f'{type(error).__name__}: {error}'
            self.failures.append({'name': 'harness-failure', 'reason': fatal})
        report = {'steps': self.steps, 'requests': self.requests, 'assertion_failures': self.failures,
                  'timings_ms': self.timings,
                  'runtime': {'bash': subprocess.check_output(['bash', '-c', 'printf "%s" "$BASH_VERSION"'], text=True),
                              'ripgrep': subprocess.check_output(['rg', '--version'], text=True).splitlines()[0],
                              'binary_sha256': hashlib.sha256(Path(BINARY).read_bytes()).hexdigest()}}
        REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'steps': len(self.steps), 'requests': self.requests, 'assertion_failures': self.failures}, ensure_ascii=False), flush=True)
        if fatal:
            raise RuntimeError(fatal)


if __name__ == '__main__':
    Suite().run()
