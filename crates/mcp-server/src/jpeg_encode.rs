//! 与参考服务兼容的 JPEG 4:2:0 编码策略及定点取整。
// Copyright 2011 The Go Authors. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
#[path = "jpeg_dct.rs"]
pub(super) mod dct;
#[path = "jpeg_tables.rs"]
pub(super) mod tables;
use image::RgbImage;
struct Encoder {
    data: Vec<u8>,
    bits: u32,
    count: u32,
    quant: [[u8; 64]; 2],
    huffman: [[u32; 256]; 4],
}
impl Encoder {
    /// 构造固定 Huffman 表和质量对应的量化表。
    fn new(quality: i32) -> Self {
        let quality = quality.clamp(1, 100);
        let scale = if quality < 50 {
            5000 / quality
        } else {
            200 - quality * 2
        };
        let mut e = Self {
            data: vec![255, 216],
            bits: 0,
            count: 0,
            quant: tables::QUANT,
            huffman: [[0; 256]; 4],
        };
        for q in &mut e.quant {
            for x in q {
                *x = ((*x as i32 * scale + 50) / 100).clamp(1, 255) as u8;
            }
        }
        for table in 0..4 {
            let (mut code, mut k) = (0u32, 0);
            for (i, count) in tables::COUNTS[table].iter().enumerate() {
                for _ in 0..*count {
                    e.huffman[table][tables::VALUES[table][k] as usize] =
                        ((i as u32 + 1) << 24) | code;
                    code += 1;
                    k += 1;
                }
                code <<= 1;
            }
        }
        e
    }
    /// 写入 JPEG marker 和大端长度。
    fn marker(&mut self, marker: u8, len: usize) {
        self.data.extend([255, marker, (len >> 8) as u8, len as u8]);
    }
    /// 写入位流；0xff 数据字节后插入零字节。
    fn emit(&mut self, mut bits: u32, mut count: u32) {
        count += self.count;
        bits <<= 32 - count;
        bits |= self.bits;
        while count >= 8 {
            let b = (bits >> 24) as u8;
            self.data.push(b);
            if b == 255 {
                self.data.push(0);
            }
            bits <<= 8;
            count -= 8;
        }
        self.bits = bits;
        self.count = count;
    }
    /// 发出 Huffman 码字。
    fn huff(&mut self, table: usize, value: usize) {
        let code = self.huffman[table][value];
        self.emit(code & 0xffffff, code >> 24);
    }
    /// 编码零游程和带符号系数。
    fn run(&mut self, table: usize, run: usize, value: i32) {
        let n = 32 - value.unsigned_abs().leading_zeros();
        self.huff(table, (run << 4) | n as usize);
        if n > 0 {
            let bits = if value < 0 { value - 1 } else { value } as u32;
            self.emit(bits & ((1 << n) - 1), n);
        }
    }
    /// 变换、量化并编码单个 8×8 数据块。
    fn block(&mut self, mut b: [i32; 64], table: usize, previous: i32) -> i32 {
        dct::forward(&mut b);
        let dc = divide(b[0], 8 * self.quant[table][0] as i32);
        self.run(2 * table, 0, dc - previous);
        let mut run = 0;
        for (zig, &index) in tables::ZIG.iter().enumerate().skip(1) {
            let ac = divide(b[index], 8 * self.quant[table][zig] as i32);
            if ac == 0 {
                run += 1;
            } else {
                while run > 15 {
                    self.huff(2 * table + 1, 240);
                    run -= 16;
                }
                self.run(2 * table + 1, run, ac);
                run = 0;
            }
        }
        if run > 0 {
            self.huff(2 * table + 1, 0);
        }
        dc
    }
}
/// 带符号整数除法按绝对值四舍五入。
fn divide(a: i32, b: i32) -> i32 {
    if a >= 0 {
        (a + (b >> 1)) / b
    } else {
        -((-a + (b >> 1)) / b)
    }
}
/// RGB 转 YCbCr，使用参考颜色库的整数系数和饱和规则。
fn ycbcr(p: &image::Rgb<u8>) -> [i32; 3] {
    let (r, g, b) = (p[0] as i32, p[1] as i32, p[2] as i32);
    [
        (19595 * r + 38470 * g + 7471 * b + (1 << 15)) >> 16,
        ((-11056 * r - 21712 * g + 32768 * b + (257 << 15)) >> 16).clamp(0, 255),
        ((32768 * r - 27440 * g - 5328 * b + (257 << 15)) >> 16).clamp(0, 255),
    ]
}
/// 编码 RGB 图像，边缘复制、色度平均和 marker 顺序与参考服务一致。
pub fn encode(image: &RgbImage, quality: i32) -> Vec<u8> {
    let (w, h) = (image.width() as usize, image.height() as usize);
    let mut e = Encoder::new(quality);
    e.marker(219, 132);
    for table in 0..2 {
        e.data.push(table as u8);
        e.data.extend(e.quant[table]);
    }
    e.marker(192, 17);
    e.data.extend([
        8,
        (h >> 8) as u8,
        h as u8,
        (w >> 8) as u8,
        w as u8,
        3,
        1,
        0x22,
        0,
        2,
        0x11,
        1,
        3,
        0x11,
        1,
    ]);
    let length = 2 + (0..4).map(|i| 17 + tables::VALUES[i].len()).sum::<usize>();
    e.marker(196, length);
    for (i, table) in [0, 16, 1, 17].into_iter().enumerate() {
        e.data.push(table);
        e.data.extend(tables::COUNTS[i]);
        e.data.extend(tables::VALUES[i]);
    }
    e.data
        .extend([255, 218, 0, 12, 3, 1, 0, 2, 17, 3, 17, 0, 63, 0]);
    let mut previous = [0; 3];
    for y in (0..h).step_by(16) {
        for x in (0..w).step_by(16) {
            let mut chroma = [[[0i32; 64]; 4]; 2];
            let (blue, red) = chroma.split_at_mut(1);
            for (i, (cb, cr)) in blue[0].iter_mut().zip(red[0].iter_mut()).enumerate() {
                let mut luma = [0; 64];
                let (dx, dy) = ((i & 1) * 8, (i & 2) * 4);
                for row in 0..8 {
                    for col in 0..8 {
                        let values = ycbcr(image.get_pixel(
                            (x + dx + col).min(w - 1) as u32,
                            (y + dy + row).min(h - 1) as u32,
                        ));
                        luma[row * 8 + col] = values[0];
                        cb[row * 8 + col] = values[1];
                        cr[row * 8 + col] = values[2];
                    }
                }
                previous[0] = e.block(luma, 0, previous[0]);
            }
            for (c, plane) in chroma.iter().enumerate() {
                let mut b = [0; 64];
                for (i, p) in plane.iter().enumerate() {
                    let off = ((i & 2) << 4) | ((i & 1) << 2);
                    for row in 0..4 {
                        for col in 0..4 {
                            let j = 16 * row + 2 * col;
                            b[8 * row + col + off] =
                                (p[j] + p[j + 1] + p[j + 8] + p[j + 9] + 2) >> 2;
                        }
                    }
                }
                previous[c + 1] = e.block(b, 1, previous[c + 1]);
            }
        }
    }
    e.emit(0x7f, 7);
    e.data.extend([255, 217]);
    e.data
}
