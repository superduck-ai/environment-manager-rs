//! JPEG 系数解码及参考实现的定点色彩转换，避免缩放前丢失通道精度。
// Copyright 2025 The Go Authors. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
use super::jpeg_encode::{dct, tables::ZIG};
use image::{DynamicImage, ImageBuffer, Rgba};
#[derive(Clone, Default)]
struct Huffman {
    codes: Vec<(u32, u8, u8)>,
    first: [u32; 17],
    counts: [u32; 17],
    offsets: [usize; 17],
}
struct Bits<'a> {
    data: &'a [u8],
    pos: usize,
    byte: u8,
    left: u8,
}
impl Bits<'_> {
    /// 读取熵编码位，跳过转义字节。
    fn read(&mut self, n: u8) -> Result<u32, String> {
        let mut value = 0;
        for _ in 0..n {
            if self.left == 0 {
                self.byte = *self.data.get(self.pos).ok_or("unexpected EOF")?;
                self.pos += 1;
                if self.byte == 255 {
                    if self.data.get(self.pos) != Some(&0) {
                        return Err("invalid JPEG format: missing 0xff00 sequence".into());
                    }
                    self.pos += 1;
                }
                self.left = 8;
            }
            self.left -= 1;
            value = (value << 1) | ((self.byte >> self.left) & 1) as u32;
        }
        Ok(value)
    }
    /// 解码规范 Huffman 符号。
    fn symbol(&mut self, h: &Huffman) -> Result<u8, String> {
        let mut value = 0;
        for n in 1..=16 {
            value = (value << 1) | self.read(1)?;
            let length = n as usize;
            if value >= h.first[length] && value - h.first[length] < h.counts[length] {
                return Ok(h.codes[h.offsets[length] + (value - h.first[length]) as usize].2);
            }
        }
        Err("invalid JPEG format: bad Huffman code".into())
    }
    /// JPEG 有符号幅度扩展。
    fn signed(&mut self, n: u8) -> Result<i32, String> {
        if n == 0 {
            return Ok(0);
        }
        let v = self.read(n)? as i32;
        Ok(if v < (1 << (n - 1)) {
            v + 1 - (1 << n)
        } else {
            v
        })
    }
}
struct Component {
    id: u8,
    h: usize,
    v: usize,
    q: usize,
    stride: usize,
    blocks: Vec<[i32; 64]>,
}
/// 读取大端标记长度。
fn word(data: &[u8], pos: usize) -> Result<usize, String> {
    let p = data.get(pos..pos + 2).ok_or("unexpected EOF")?;
    Ok(u16::from_be_bytes([p[0], p[1]]) as usize)
}
/// 解码顺序或渐进 JPEG，返回具有完整 16 位色彩精度的像素。
pub fn decode(data: &[u8]) -> Result<DynamicImage, String> {
    let mut pos = 2;
    let (mut width, mut height) = (0, 0);
    let mut components: Vec<Component> = Vec::new();
    let mut quant = [[0i32; 64]; 4];
    let mut huff = vec![Huffman::default(); 8];
    let mut restart = 0;
    let mut progressive = false;
    let mut rgb = false;
    let mut adobe = None;
    let mut jfif = false;
    while pos < data.len() {
        if data[pos] != 255 {
            pos += 1;
            continue;
        }
        while data.get(pos) == Some(&255) {
            pos += 1;
        }
        let marker = *data.get(pos).ok_or("unexpected EOF")?;
        pos += 1;
        if marker == 0xd9 {
            break;
        }
        let len = word(data, pos)?;
        if len < 2 {
            return Err("invalid JPEG format: short segment length".into());
        }
        pos += 2;
        let seg = data.get(pos..pos + len - 2).ok_or("unexpected EOF")?;
        pos += len - 2;
        match marker {
            0xc0..=0xc2 => {
                progressive = marker == 0xc2;
                if seg.len() < 6 {
                    return Err("unexpected EOF".into());
                }
                height = word(seg, 1)?;
                width = word(seg, 3)?;
                let n = seg[5] as usize;
                if ![1, 3, 4].contains(&n) || seg.len() != 6 + 3 * n {
                    return Err("invalid JPEG format: SOF has wrong length".into());
                }
                let maxh = (0..n)
                    .map(|i| (seg[7 + 3 * i] >> 4) as usize)
                    .max()
                    .unwrap();
                let maxv = (0..n)
                    .map(|i| (seg[7 + 3 * i] & 15) as usize)
                    .max()
                    .unwrap();
                if maxh == 0 || maxv == 0 {
                    return Err("invalid JPEG format: zero sampling factor".into());
                }
                let mx = width.div_ceil(8 * maxh);
                let my = height.div_ceil(8 * maxv);
                for i in 0..n {
                    let h = (seg[7 + 3 * i] >> 4) as usize;
                    let v = (seg[7 + 3 * i] & 15) as usize;
                    if h == 0 || v == 0 {
                        return Err("invalid JPEG format: zero sampling factor".into());
                    }
                    components.push(Component {
                        id: seg[6 + 3 * i],
                        h,
                        v,
                        q: seg[8 + 3 * i] as usize,
                        stride: mx * h,
                        blocks: vec![[0; 64]; mx * h * my * v],
                    });
                }
                rgb = n == 3 && components.iter().map(|c| c.id).eq(b"RGB".iter().copied());
            }
            0xdb => {
                let mut p = 0;
                while p < seg.len() {
                    let info = seg[p];
                    p += 1;
                    let q = (info & 15) as usize;
                    if q >= 4 {
                        return Err("invalid JPEG format: bad Tq value".into());
                    }
                    for value in &mut quant[q] {
                        *value = if info >> 4 == 0 {
                            let v = *seg.get(p).ok_or("unexpected EOF")?;
                            p += 1;
                            v as i32
                        } else {
                            let v = word(seg, p)?;
                            p += 2;
                            v as i32
                        };
                    }
                }
            }
            0xc4 => {
                let mut p = 0;
                while p < seg.len() {
                    let info = seg[p];
                    p += 1;
                    let index = ((info >> 4) as usize) * 4 + (info & 15) as usize;
                    if index >= 8 {
                        return Err("invalid JPEG format: bad Huffman table".into());
                    }
                    let counts = seg.get(p..p + 16).ok_or("unexpected EOF")?;
                    p += 16;
                    let mut code = 0;
                    let mut table = Huffman::default();
                    for (i, n) in counts.iter().enumerate() {
                        table.first[i + 1] = code;
                        table.counts[i + 1] = *n as u32;
                        table.offsets[i + 1] = table.codes.len();
                        for _ in 0..*n {
                            let value = *seg.get(p).ok_or("unexpected EOF")?;
                            p += 1;
                            table.codes.push((code, (i + 1) as u8, value));
                            code += 1;
                        }
                        code <<= 1;
                    }
                    huff[index] = table;
                }
            }
            0xdd => restart = word(seg, 0)?,
            0xe0 => {
                jfif = seg.starts_with(b"JFIF\0");
            }
            0xee => {
                if seg.starts_with(b"Adobe") && seg.len() >= 12 {
                    adobe = Some(seg[11]);
                }
            }
            0xda => {
                let n = *seg.first().ok_or("unexpected EOF")? as usize;
                if seg.len() != 1 + 2 * n + 3 {
                    return Err("invalid JPEG format: SOS has wrong length".into());
                }
                let mut scans = Vec::new();
                for i in 0..n {
                    let ci = components
                        .iter()
                        .position(|c| c.id == seg[1 + 2 * i])
                        .ok_or("invalid JPEG format: unknown component")?;
                    let table = seg[2 + 2 * i];
                    if table >> 4 >= 4 || table & 15 >= 4 {
                        return Err("invalid JPEG format: bad Huffman table".into());
                    }
                    scans.push((ci, (table >> 4) as usize, (table & 15) as usize + 4));
                }
                let start = if progressive {
                    seg[1 + 2 * n] as usize
                } else {
                    0
                };
                let end = if progressive {
                    seg[2 + 2 * n] as usize
                } else {
                    63
                };
                let high = if progressive { seg[3 + 2 * n] >> 4 } else { 0 };
                let low = if progressive { seg[3 + 2 * n] & 15 } else { 0 };
                if start > end || end >= 64 {
                    return Err("invalid JPEG format: bad spectral selection bounds".into());
                }
                let maxh = components.iter().map(|c| c.h).max().unwrap();
                let maxv = components.iter().map(|c| c.v).max().unwrap();
                let mx = width.div_ceil(8 * maxh);
                let my = height.div_ceil(8 * maxv);
                let (nx, ny) = if n == 1 {
                    let c = &components[scans[0].0];
                    (
                        (width * c.h).div_ceil(8 * maxh),
                        (height * c.v).div_ceil(8 * maxv),
                    )
                } else {
                    (mx, my)
                };
                let mut bits = Bits {
                    data,
                    pos,
                    byte: 0,
                    left: 0,
                };
                let mut dc = vec![0i32; components.len()];
                let mut eob = 0u32;
                for mcu in 0..nx * ny {
                    if restart > 0 && mcu > 0 && mcu % restart == 0 {
                        bits.left = 0;
                        while data.get(bits.pos) == Some(&255) {
                            bits.pos += 1;
                        }
                        let mark = *data.get(bits.pos).ok_or("unexpected EOF")?;
                        if !(0xd0..=0xd7).contains(&mark) {
                            return Err("invalid JPEG format: bad RST marker".into());
                        }
                        bits.pos += 1;
                        dc.fill(0);
                        eob = 0;
                    }
                    for &(ci, dh, ah) in &scans {
                        let c = &mut components[ci];
                        let (bh, bv) = if n == 1 { (1, 1) } else { (c.h, c.v) };
                        for y in 0..bv {
                            for x in 0..bh {
                                let bx = mcu % nx * bh + x;
                                let by = mcu / nx * bv + y;
                                let block = c
                                    .blocks
                                    .get_mut(by * c.stride + bx)
                                    .ok_or("invalid JPEG format: too many blocks")?;
                                if high != 0 {
                                    refine(
                                        &mut bits,
                                        &huff[ah],
                                        block,
                                        start,
                                        end,
                                        1 << low,
                                        &mut eob,
                                    )?;
                                    continue;
                                }
                                let mut k = start;
                                if k == 0 {
                                    let t = bits.symbol(&huff[dh])?;
                                    if t > 16 {
                                        return Err(
                                            "invalid JPEG format: excessive DC component".into()
                                        );
                                    }
                                    dc[ci] = dc[ci].wrapping_add(bits.signed(t)?);
                                    block[0] = dc[ci] << low;
                                    k = 1;
                                }
                                if k > end {
                                    continue;
                                }
                                if eob > 0 {
                                    eob -= 1;
                                    continue;
                                }
                                while k <= end {
                                    let symbol = bits.symbol(&huff[ah])?;
                                    let run = (symbol >> 4) as usize;
                                    let size = symbol & 15;
                                    if size == 0 {
                                        if run == 15 {
                                            k += 16;
                                            continue;
                                        }
                                        eob = (1 << run) + bits.read(run as u8)? - 1;
                                        break;
                                    }
                                    k += run;
                                    if k > end {
                                        return Err(
                                            "invalid JPEG format: too many coefficients".into()
                                        );
                                    }
                                    block[ZIG[k]] = bits.signed(size)? << low;
                                    k += 1;
                                }
                            }
                        }
                    }
                }
                pos = bits.pos;
            }
            _ => {}
        }
    }
    if components.is_empty() {
        return Err("invalid JPEG format: missing SOF marker".into());
    }
    let maxh = components.iter().map(|c| c.h).max().unwrap();
    let maxv = components.iter().map(|c| c.v).max().unwrap();
    let mut planes = Vec::new();
    for c in &components {
        let mut plane = vec![0u8; c.blocks.len() * 64];
        for (i, b) in c.blocks.iter().enumerate() {
            let mut b = *b;
            if c.q >= 4 {
                return Err("invalid JPEG format: bad Tq value".into());
            }
            for z in 0..64 {
                b[ZIG[z]] = b[ZIG[z]].wrapping_mul(quant[c.q][z]);
            }
            dct::inverse(&mut b);
            let bx = i % c.stride;
            let by = i / c.stride;
            for y in 0..8 {
                for x in 0..8 {
                    plane[(by * 8 + y) * c.stride * 8 + bx * 8 + x] =
                        b[y * 8 + x].saturating_add(128).clamp(0, 255) as u8;
                }
            }
        }
        planes.push(plane);
    }
    let mut output = ImageBuffer::new(width as u32, height as u32);
    for (x, y, p) in output.enumerate_pixels_mut() {
        let samples: Vec<u8> = components
            .iter()
            .zip(&planes)
            .map(|(c, plane)| {
                plane[(y as usize * c.v / maxv) * c.stride * 8 + x as usize * c.h / maxh]
            })
            .collect();
        let color = if samples.len() == 1 {
            [samples[0] as u16 * 257; 3]
        } else if !jfif && (rgb || adobe == Some(0)) {
            samples[..3]
                .try_into()
                .map(|p: [u8; 3]| p.map(|v| v as u16 * 257))
                .unwrap()
        } else {
            ycbcr(samples[0], samples[1], samples[2])
        };
        let color = if samples.len() == 4 {
            let transform = adobe.ok_or("unsupported JPEG feature: unknown color model: 4-component JPEG doesn't have Adobe APP14 metadata")?;
            let k = samples[3] as u32;
            if transform == 0 {
                [samples[0], samples[1], samples[2]].map(|v| (v as u32 * 257 * k / 255) as u16)
            } else {
                color.map(|v| ((255 - (v as u32 >> 8)) * 257 * k / 255) as u16)
            }
        } else {
            color
        };
        *p = Rgba([color[0], color[1], color[2], 65535]);
    }
    Ok(DynamicImage::ImageRgba16(output))
}
/// 按参考 RGBA 方法保留 YCbCr 转换的小数精度。
pub(super) fn ycbcr(y: u8, cb: u8, cr: u8) -> [u16; 3] {
    let yy = y as i32 * 0x10101;
    let cb = cb as i32 - 128;
    let cr = cr as i32 - 128;
    [
        yy + 91881 * cr,
        yy - 22554 * cb - 46802 * cr,
        yy + 116130 * cb,
    ]
    .map(|v| (v >> 8).clamp(0, 65535) as u16)
}
/// 逐次逼近时更新已有非零 AC 系数，并跳过指定数量零系数。
fn refine_nonzero(
    bits: &mut Bits<'_>,
    b: &mut [i32; 64],
    mut k: usize,
    end: usize,
    mut skip: i32,
    delta: i32,
) -> Result<usize, String> {
    while k <= end {
        let v = &mut b[ZIG[k]];
        if *v == 0 {
            if skip == 0 {
                break;
            }
            skip -= 1;
        } else if bits.read(1)? != 0 {
            *v += if *v >= 0 { delta } else { -delta };
        }
        k += 1;
    }
    Ok(k)
}
/// 解码渐进 JPEG 的 DC 或 AC 精细化扫描。
fn refine(
    bits: &mut Bits<'_>,
    h: &Huffman,
    b: &mut [i32; 64],
    start: usize,
    end: usize,
    delta: i32,
    eob: &mut u32,
) -> Result<(), String> {
    if start == 0 {
        if bits.read(1)? != 0 {
            b[0] |= delta;
        }
        return Ok(());
    }
    let mut k = start;
    if *eob == 0 {
        while k <= end {
            let symbol = bits.symbol(h)?;
            let run = symbol >> 4;
            let size = symbol & 15;
            let mut value = 0;
            if size == 0 {
                if run != 15 {
                    *eob = (1 << run) + bits.read(run)?;
                    break;
                }
            } else if size == 1 {
                value = if bits.read(1)? != 0 { delta } else { -delta };
            } else {
                return Err("invalid JPEG format: unexpected Huffman code".into());
            }
            k = refine_nonzero(bits, b, k, end, run as i32, delta)?;
            if k > end {
                return Err("invalid JPEG format: too many coefficients".into());
            }
            if value != 0 {
                b[ZIG[k]] = value;
            }
            k += 1;
        }
    }
    if *eob > 0 {
        *eob -= 1;
        refine_nonzero(bits, b, k, end, -1, delta)?;
    }
    Ok(())
}
