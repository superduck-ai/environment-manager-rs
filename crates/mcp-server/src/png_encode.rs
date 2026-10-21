//! PNG 自适应过滤与 DEFLATE 兼容编码，保留参考编码器的决策和位流。
// Copyright 2009 The Go Authors. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
use image::RgbaImage;
const LENGTH: [usize; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LE: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DIST: [usize; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
];
const DE: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
const ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];
#[derive(Clone, Copy, Default)]
struct Code {
    value: u16,
    length: u8,
}
#[derive(Clone, Copy, Default)]
struct Level {
    last: i32,
    next: i32,
    pair: i32,
    needed: i32,
}
/// 构造长度受限的规范 Huffman 编码，频率相同时按符号排序。
fn codes(freq: &[i32], maxbits: usize) -> Vec<Code> {
    let mut output = vec![Code::default(); freq.len()];
    let mut list: Vec<(usize, i32)> = freq
        .iter()
        .enumerate()
        .filter(|(_, f)| **f > 0)
        .map(|(i, f)| (i, *f))
        .collect();
    let n = list.len();
    if n <= 2 {
        for (i, (symbol, _)) in list.iter().enumerate() {
            output[*symbol] = Code {
                value: i as u16,
                length: 1,
            };
        }
        return output;
    }
    list.sort_by_key(|(s, f)| (*f, *s));
    let maxbits = maxbits.min(n - 1);
    list.push((usize::MAX, i32::MAX));
    let mut levels = [Level::default(); 17];
    let mut leaves = [[0i32; 17]; 17];
    for l in 1..=maxbits {
        levels[l] = Level {
            last: list[1].1,
            next: list[2].1,
            pair: if l == 1 {
                i32::MAX
            } else {
                list[0].1 + list[1].1
            },
            needed: 0,
        };
        leaves[l][l] = 2;
    }
    levels[maxbits].needed = 2 * n as i32 - 4;
    let mut l = maxbits;
    loop {
        if levels[l].pair == i32::MAX && levels[l].next == i32::MAX {
            levels[l].needed = 0;
            levels[l + 1].pair = i32::MAX;
            l += 1;
            continue;
        }
        let previous = levels[l].last;
        if levels[l].next < levels[l].pair {
            leaves[l][l] += 1;
            levels[l].last = levels[l].next;
            levels[l].next = list[leaves[l][l] as usize].1;
        } else {
            levels[l].last = levels[l].pair;
            let lower = leaves[l - 1];
            leaves[l][..l].copy_from_slice(&lower[..l]);
            levels[l - 1].needed = 2;
        }
        levels[l].needed -= 1;
        if levels[l].needed == 0 {
            if l == maxbits {
                break;
            }
            levels[l + 1].pair = previous + levels[l].last;
            l += 1;
        } else {
            while levels[l - 1].needed > 0 {
                l -= 1;
            }
        }
    }
    list.pop();
    let mut counts = [0usize; 17];
    for l in (1..=maxbits).rev() {
        counts[maxbits - l + 1] = (leaves[maxbits][l] - leaves[maxbits][l - 1]) as usize;
    }
    let mut code = 0u16;
    for (bits, count) in counts.iter().enumerate() {
        code <<= 1;
        if *count == 0 {
            continue;
        }
        let begin = list.len() - count;
        list[begin..].sort_by_key(|(s, _)| *s);
        for &(symbol, _) in &list[begin..] {
            output[symbol] = Code {
                value: code.reverse_bits() >> (16 - bits),
                length: bits as u8,
            };
            code += 1;
        }
        list.truncate(begin);
    }
    output
}
struct Writer {
    data: Vec<u8>,
    bits: u64,
    count: u8,
}
impl Writer {
    /// 按低位优先写入 DEFLATE 位流。
    fn bits(&mut self, value: u32, n: u8) {
        self.bits |= (value as u64) << self.count;
        self.count += n;
        while self.count >= 8 {
            self.data.push(self.bits as u8);
            self.bits >>= 8;
            self.count -= 8;
        }
    }
    /// 写入单个 Huffman 码。
    fn code(&mut self, c: Code) {
        self.bits(c.value as u32, c.length);
    }
    /// 对齐字节边界。
    fn align(&mut self) {
        if self.count > 0 {
            self.data.push(self.bits as u8);
            self.bits = 0;
            self.count = 0;
        }
    }
    /// 写入存储块。
    fn stored(&mut self, data: &[u8], last: bool) {
        self.bits(u32::from(last), 3);
        self.align();
        self.data
            .extend_from_slice(&(data.len() as u16).to_le_bytes());
        self.data
            .extend_from_slice(&(!(data.len() as u16)).to_le_bytes());
        self.data.extend_from_slice(data);
    }
}
#[derive(Clone, Copy)]
enum Token {
    Literal(u16),
    Match(usize, usize),
}
/// 找出长度或距离所属的区间代码。
fn index(value: usize, bases: &[usize]) -> usize {
    bases.iter().rposition(|b| *b <= value).unwrap()
}
/// 将一组 LZ77 标记编码为最短的存储、固定或动态 Huffman 块。
fn block(w: &mut Writer, tokens: &[Token], input: Option<&[u8]>) {
    let mut lf = vec![0i32; 286];
    let mut df = vec![0i32; 30];
    let mut extra = 0usize;
    for token in tokens {
        match *token {
            Token::Literal(v) => lf[v as usize] += 1,
            Token::Match(n, d) => {
                let l = index(n, &LENGTH);
                let o = index(d, &DIST);
                lf[257 + l] += 1;
                df[o] += 1;
                extra += LE[l] as usize + DE[o] as usize;
            }
        }
    }
    lf[256] += 1;
    if df.iter().all(|f| *f == 0) {
        df[0] = 1;
    }
    let nl = lf.iter().rposition(|f| *f > 0).unwrap() + 1;
    let nd = df.iter().rposition(|f| *f > 0).unwrap() + 1;
    let lc = codes(&lf, 15);
    let dc = codes(&df, 15);
    let lengths: Vec<u8> = lc[..nl].iter().chain(&dc[..nd]).map(|c| c.length).collect();
    let mut runs = Vec::new();
    let mut cf = vec![0i32; 19];
    let mut i = 0;
    while i < lengths.len() {
        let value = lengths[i];
        let mut n = 1;
        while i + n < lengths.len() && lengths[i + n] == value {
            n += 1;
        }
        i += n;
        if value != 0 {
            runs.push((value, 0));
            cf[value as usize] += 1;
            n -= 1;
            while n >= 3 {
                let count = n.min(6);
                runs.push((16, (count - 3) as u8));
                cf[16] += 1;
                n -= count;
            }
        } else {
            while n >= 11 {
                let count = n.min(138);
                runs.push((18, (count - 11) as u8));
                cf[18] += 1;
                n -= count;
            }
            if n >= 3 {
                runs.push((17, (n - 3) as u8));
                cf[17] += 1;
                n = 0;
            }
        }
        for _ in 0..n {
            runs.push((value, 0));
            cf[value as usize] += 1;
        }
    }
    let cc = codes(&cf, 7);
    let nc = (4..=19)
        .rev()
        .find(|n| cc[ORDER[n - 1]].length != 0)
        .unwrap_or(4);
    let cost = |f: &[i32], c: &[Code]| {
        f.iter()
            .zip(c)
            .map(|(f, c)| *f as usize * c.length as usize)
            .sum::<usize>()
    };
    let dynamic = 3
        + 5
        + 5
        + 4
        + 3 * nc
        + cost(&cf, &cc)
        + cf[16] as usize * 2
        + cf[17] as usize * 3
        + cf[18] as usize * 7
        + cost(&lf, &lc)
        + cost(&df, &dc)
        + extra;
    let fixed: Vec<Code> = (0..288)
        .map(|s| {
            let (v, n) = if s < 144 {
                (s + 48, 8)
            } else if s < 256 {
                (s + 256, 9)
            } else if s < 280 {
                (s - 256, 7)
            } else {
                (s - 88, 8)
            };
            Code {
                value: (v as u16).reverse_bits() >> (16 - n),
                length: n,
            }
        })
        .collect();
    let offsets: Vec<Code> = (0u16..30)
        .map(|s| Code {
            value: s.reverse_bits() >> 11,
            length: 5,
        })
        .collect();
    let fixedsize = 3 + cost(&lf, &fixed) + cost(&df, &offsets) + extra;
    if let Some(input) = input
        && input.len() <= 65535
        && (input.len() + 5) * 8 < dynamic.min(fixedsize)
    {
        w.stored(input, false);
        return;
    }
    let (lc, dc) = if dynamic < fixedsize {
        w.bits(4, 3);
        w.bits((nl - 257) as u32, 5);
        w.bits((nd - 1) as u32, 5);
        w.bits((nc - 4) as u32, 4);
        for o in &ORDER[..nc] {
            w.bits(cc[*o].length as u32, 3);
        }
        for (code, value) in runs {
            w.code(cc[code as usize]);
            if code >= 16 {
                w.bits(
                    value as u32,
                    match code {
                        16 => 2,
                        17 => 3,
                        _ => 7,
                    },
                );
            }
        }
        (lc, dc)
    } else {
        w.bits(2, 3);
        (fixed, offsets)
    };
    for token in tokens {
        match *token {
            Token::Literal(v) => w.code(lc[v as usize]),
            Token::Match(n, d) => {
                let l = index(n, &LENGTH);
                let o = index(d, &DIST);
                w.code(lc[257 + l]);
                w.bits((n - LENGTH[l]) as u32, LE[l]);
                w.code(dc[o]);
                w.bits((d - DIST[o]) as u32, DE[o]);
            }
        }
    }
    w.code(lc[256]);
}
/// 为四字节窗口计算参考哈希。
fn hash(data: &[u8], pos: usize) -> usize {
    u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap())
        .wrapping_mul(0x1e35a7bd)
        .wrapping_shr(15) as usize
}
/// 使用 32 KiB 历史和延迟匹配生成参考最佳压缩位流。
fn deflate(data: &[u8]) -> Vec<u8> {
    let mut w = Writer {
        data: Vec::new(),
        bits: 0,
        count: 0,
    };
    let mut head = vec![usize::MAX; 1 << 17];
    let mut prev = vec![usize::MAX; 32768];
    let mut tokens = Vec::new();
    let (mut pos, mut start) = (0, 0);
    let (mut length, mut offset) = (3, 0);
    let mut available = false;
    while pos < data.len() {
        let oldlength = length;
        let oldoffset = offset;
        length = 3;
        offset = 0;
        let mut chain = usize::MAX;
        if pos + 3 < data.len() {
            let h = hash(data, pos);
            chain = head[h];
            prev[pos & 32767] = chain;
            head[h] = pos;
        }
        let look = (data.len() - pos).min(258);
        if chain != usize::MAX && look > oldlength && oldlength < 258 {
            let min = pos.saturating_sub(32768);
            let mut tries = 4096;
            while chain >= min && chain < pos && tries > 0 {
                tries -= 1;
                if data[pos + length] == data[chain + length] {
                    let mut n = 0;
                    while n < look && data[pos + n] == data[chain + n] {
                        n += 1;
                    }
                    if n > length && (n > 4 || pos - chain <= 4096) {
                        length = n;
                        offset = pos - chain;
                        if n == look {
                            break;
                        }
                    }
                }
                if chain == min {
                    break;
                }
                let next = prev[chain & 32767];
                if next >= chain {
                    break;
                }
                chain = next;
            }
        }
        let end;
        if oldlength >= 4 && length <= oldlength {
            tokens.push(Token::Match(oldlength, oldoffset));
            end = pos + oldlength - 1;
            for at in pos + 1..end {
                if at + 3 < data.len() {
                    let h = hash(data, at);
                    prev[at & 32767] = head[h];
                    head[h] = at;
                }
            }
            pos = end;
            length = 3;
            available = false;
        } else {
            end = pos;
            if available {
                tokens.push(Token::Literal(data[pos - 1] as u16));
            }
            pos += 1;
            available = true;
        }
        if tokens.len() == 16384 {
            let input = if end - start <= 65535 {
                Some(&data[start..end])
            } else {
                None
            };
            block(&mut w, &tokens, input);
            tokens.clear();
            start = end;
        }
    }
    if available {
        tokens.push(Token::Literal(data[pos - 1] as u16));
    }
    if !tokens.is_empty() {
        block(
            &mut w,
            &tokens,
            if pos - start <= 65535 {
                Some(&data[start..pos])
            } else {
                None
            },
        );
    }
    w.stored(&[], true);
    w.data
}
/// 计算 PNG 块 CRC32。
fn crc(data: &[u8]) -> u32 {
    let mut c = u32::MAX;
    for b in data {
        c ^= *b as u32;
        for _ in 0..8 {
            c = (c >> 1) ^ if c & 1 != 0 { 0xedb88320 } else { 0 };
        }
    }
    !c
}
/// 写入 PNG 数据块。
fn chunk(out: &mut Vec<u8>, name: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    let begin = out.len();
    out.extend_from_slice(name);
    out.extend_from_slice(data);
    out.extend_from_slice(&crc(&out[begin..]).to_be_bytes());
}
/// 对预乘 RGBA 图像应用自适应过滤，再封装 zlib 和 PNG。
pub fn encode(image: &RgbaImage) -> Vec<u8> {
    let opaque = image.pixels().all(|p| p[3] == 255);
    let bpp = if opaque { 3 } else { 4 };
    let image = super::image_resize::unpremultiply(image);
    let mut filtered = Vec::new();
    let mut previous = vec![0u8; image.width() as usize * bpp];
    for row in image.rows() {
        let row: Vec<u8> = row.flat_map(|p| p.0[..bpp].to_vec()).collect();
        let mut best = usize::MAX;
        let mut selected = Vec::new();
        for filter in [2, 4, 0, 1, 3] {
            let mut bytes = Vec::with_capacity(row.len() + 1);
            bytes.push(filter);
            let mut score = 0;
            for (i, v) in row.iter().enumerate() {
                let a = if i >= bpp { row[i - bpp] } else { 0 };
                let b = previous[i];
                let c = if i >= bpp { previous[i - bpp] } else { 0 };
                let predict = match filter {
                    0 => 0,
                    1 => a,
                    2 => b,
                    3 => ((a as u16 + b as u16) / 2) as u8,
                    _ => {
                        let p = a as i32 + b as i32 - c as i32;
                        let pa = (p - a as i32).abs();
                        let pb = (p - b as i32).abs();
                        let pc = (p - c as i32).abs();
                        if pa <= pb && pa <= pc {
                            a
                        } else if pb <= pc {
                            b
                        } else {
                            c
                        }
                    }
                };
                let v = v.wrapping_sub(predict);
                score += if v < 128 {
                    v as usize
                } else {
                    256 - v as usize
                };
                bytes.push(v);
            }
            if score < best {
                best = score;
                selected = bytes;
            }
        }
        filtered.extend_from_slice(&selected);
        previous = row;
    }
    let mut zlib = vec![0x78, 0xda];
    zlib.extend_from_slice(&deflate(&filtered));
    let (mut a, mut b) = (1u32, 0u32);
    for v in filtered {
        a = (a + v as u32) % 65521;
        b = (b + a) % 65521;
    }
    zlib.extend_from_slice(&((b << 16) | a).to_be_bytes());
    let mut out = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut header = Vec::new();
    header.extend_from_slice(&image.width().to_be_bytes());
    header.extend_from_slice(&image.height().to_be_bytes());
    header.extend_from_slice(&[8, if opaque { 2 } else { 6 }, 0, 0, 0]);
    chunk(&mut out, b"IHDR", &header);
    for data in zlib.chunks(32768) {
        chunk(&mut out, b"IDAT", data);
    }
    chunk(&mut out, b"IEND", &[]);
    out
}
