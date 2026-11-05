//! JPEG 定点离散余弦变换，保留整数取整及中间精度。
// Copyright 2025 The Go Authors. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.

const COS1: u64 = 1130768441178740757;
const SIN1: u64 = 224923827593068887;
const COS3: u64 = 958619196450722178;
const SIN3: u64 = 640528868967736374;
const SQRT2: u64 = 1630477228166597777;
const SQRT2_COS6: u64 = 623956622067911264;
const SQRT2_SIN6: u64 = 1506364539328854985;
/// 将高精度常量取整到所需定点位数。
fn c(x: u64, bits: u32) -> i32 {
    ((x + (1 << (59 - bits))) >> (60 - bits)) as i32
}
/// 使用三次乘法进行旋转及缩放。
fn dct_box(x0: i32, x1: i32, kcos: i32, ksin: i32) -> (i32, i32) {
    let sum = kcos * (x0 + x1);
    (sum + (ksin - kcos) * x1, sum - (kcos + ksin) * x0)
}
/// 进行二维正向离散余弦变换。
pub fn forward(b: &mut [i32; 64]) {
    cols(b);
    rows(b);
}
/// 沿单个轴进行定点变换。
fn cols(b: &mut [i32; 64]) {
    for i in 0..8 {
        let mut x0 = b[i];
        let mut x1 = b[8 + i];
        let mut x2 = b[2 * 8 + i];
        let mut x3 = b[3 * 8 + i];
        let mut x4 = b[4 * 8 + i];
        let mut x5 = b[5 * 8 + i];
        let mut x6 = b[6 * 8 + i];
        let mut x7 = b[7 * 8 + i];
        (x0, x7) = (x0 + x7, x0 - x7);
        (x1, x6) = (x1 + x6, x1 - x6);
        (x2, x5) = (x2 + x5, x2 - x5);
        (x3, x4) = (x3 + x4, x3 - x4);
        (x4, x7) = dct_box(x4, x7, c(COS3, 18), c(SIN3, 18));
        (x5, x6) = dct_box(x5, x6, c(COS1, 18), c(SIN1, 18));
        (x0, x3) = (x0 + x3, x0 - x3);
        (x1, x2) = (x1 + x2, x1 - x2);
        (x2, x3) = dct_box(x2, x3, c(SQRT2_COS6, 18), c(SQRT2_SIN6, 18));
        (x0, x1) = (x0 + x1, x0 - x1);
        b[i] = (x0 - 128 * 8) << 18;
        b[4 * 8 + i] = x1 << 18;
        b[2 * 8 + i] = x2;
        b[6 * 8 + i] = x3;
        (x4, x6) = (x4 + x6, x4 - x6);
        (x7, x5) = (x7 + x5, x7 - x5);
        x5 = (x5 >> 12) * c(SQRT2, 12);
        x6 = (x6 >> 12) * c(SQRT2, 12);
        (x7, x4) = (x7 + x4, x7 - x4);
        b[8 + i] = x7;
        b[3 * 8 + i] = x5;
        b[5 * 8 + i] = x6;
        b[7 * 8 + i] = x4;
    }
}
/// 沿单个轴进行定点变换。
fn rows(b: &mut [i32; 64]) {
    for i in 0..8 {
        let x = &mut b[8 * i..8 * i + 8];
        let mut x0 = x[0];
        let mut x1 = x[1];
        let mut x2 = x[2];
        let mut x3 = x[3];
        let mut x4 = x[4];
        let mut x5 = x[5];
        let mut x6 = x[6];
        let mut x7 = x[7];
        (x0, x7) = (x0 + x7, x0 - x7);
        (x1, x6) = (x1 + x6, x1 - x6);
        (x2, x5) = (x2 + x5, x2 - x5);
        (x3, x4) = (x3 + x4, x3 - x4);
        (x4, x7) = dct_box(x4 >> 14, x7 >> 14, c(COS3, 14), c(SIN3, 14));
        (x5, x6) = dct_box(x5 >> 14, x6 >> 14, c(COS1, 14), c(SIN1, 14));
        (x0, x3) = (x0 + x3, x0 - x3);
        (x1, x2) = (x1 + x2, x1 - x2);
        (x2, x3) = dct_box(x2 >> 14, x3 >> 14, c(SQRT2_COS6, 14), c(SQRT2_SIN6, 14));
        (x0, x1) = (x0 + x1, x0 - x1);
        (x4, x6) = (x4 + x6, x4 - x6);
        (x7, x5) = (x7 + x5, x7 - x5);
        x5 = (x5 >> 14) * c(SQRT2, 14);
        x6 = (x6 >> 14) * c(SQRT2, 14);
        (x7, x4) = (x7 + x4, x7 - x4);
        x0 = (x0 + (1 << 17)) >> 18;
        x1 = (x1 + (1 << 17)) >> 18;
        x2 = (x2 + (1 << 17)) >> 18;
        x3 = (x3 + (1 << 17)) >> 18;
        x4 = (x4 + (1 << 17)) >> 18;
        x5 = (x5 + (1 << 17)) >> 18;
        x6 = (x6 + (1 << 17)) >> 18;
        x7 = (x7 + (1 << 17)) >> 18;
        x[0] = x0;
        x[1] = x7;
        x[2] = x2;
        x[3] = x5;
        x[4] = x1;
        x[5] = x6;
        x[6] = x3;
        x[7] = x4;
    }
}

const SQRT2INV: u64 = 815238614083298888;
const SQRT2INV_COS6: u64 = 311978311033955632;
const SQRT2INV_SIN6: u64 = 753182269664427492;
/// 逆向定点变换，沿行再沿列还原像素。
pub fn inverse(b: &mut [i32; 64]) {
    inverse_rows(b);
    inverse_cols(b);
}

/// 沿单个轴进行逆变换并保持参考取整。
fn inverse_rows(b: &mut [i32; 64]) {
    for i in 0..8 {
        let x = &mut b[8 * i..8 * i + 8];
        let mut x0 = std::num::Wrapping(x[0]);
        let mut x7 = std::num::Wrapping(x[1]);
        let mut x2 = std::num::Wrapping(x[2]);
        let mut x5 = std::num::Wrapping(x[3]);
        let mut x1 = std::num::Wrapping(x[4]);
        let mut x6 = std::num::Wrapping(x[5]);
        let mut x3 = std::num::Wrapping(x[6]);
        let mut x4 = std::num::Wrapping(x[7]);
        x0 <<= 17;
        x1 <<= 17;
        (x0, x1) = (x0 + x1, x0 - x1);
        (x2, x3) = inverse_box(
            x2,
            x3,
            std::num::Wrapping(c(SQRT2INV_COS6, 18)),
            -std::num::Wrapping(c(SQRT2INV_SIN6, 18)),
        );
        (x1, x2) = (x1 + x2, x1 - x2);
        (x0, x3) = (x0 + x3, x0 - x3);
        x4 <<= 7;
        x7 <<= 7;
        (x7, x4) = (x7 + x4, x7 - x4);
        x6 *= std::num::Wrapping(c(SQRT2INV, 8));
        x5 *= std::num::Wrapping(c(SQRT2INV, 8));
        (x7, x5) = (x7 + x5, x7 - x5);
        (x4, x6) = (x4 + x6, x4 - x6);
        (x4, x7) = inverse_box(
            x4 >> 2,
            x7 >> 2,
            std::num::Wrapping(c(COS3, 12)),
            -std::num::Wrapping(c(SIN3, 12)),
        );
        (x5, x6) = inverse_box(
            x5 >> 2,
            x6 >> 2,
            std::num::Wrapping(c(COS1, 12)),
            -std::num::Wrapping(c(SIN1, 12)),
        );
        (x0, x7) = (x0 + x7, x0 - x7);
        (x1, x6) = (x1 + x6, x1 - x6);
        (x2, x5) = (x2 + x5, x2 - x5);
        (x3, x4) = (x3 + x4, x3 - x4);
        x[0] = x0.0;
        x[1] = x1.0;
        x[2] = x2.0;
        x[3] = x3.0;
        x[4] = x4.0;
        x[5] = x5.0;
        x[6] = x6.0;
        x[7] = x7.0;
    }
}

/// 沿单个轴进行逆变换并保持参考取整。
fn inverse_cols(b: &mut [i32; 64]) {
    for i in 0..8 {
        let mut x0 = std::num::Wrapping(b[i]);
        let mut x7 = std::num::Wrapping(b[8 + i]);
        let mut x2 = std::num::Wrapping(b[2 * 8 + i]);
        let mut x5 = std::num::Wrapping(b[3 * 8 + i]);
        let mut x1 = std::num::Wrapping(b[4 * 8 + i]);
        let mut x6 = std::num::Wrapping(b[5 * 8 + i]);
        let mut x3 = std::num::Wrapping(b[6 * 8 + i]);
        let mut x4 = std::num::Wrapping(b[7 * 8 + i]);
        x0 += std::num::Wrapping(1 << 19);
        (x0, x1) = ((x0 + x1) >> 2, (x0 - x1) >> 2);
        (x2, x3) = inverse_box(
            x2 >> 13,
            x3 >> 13,
            std::num::Wrapping(c(SQRT2INV_COS6, 12)),
            -std::num::Wrapping(c(SQRT2INV_SIN6, 12)),
        );
        (x1, x2) = (x1 + x2, x1 - x2);
        (x0, x3) = (x0 + x3, x0 - x3);
        (x7, x4) = (x7 + x4, x7 - x4);
        x5 = (x5 >> 13) * std::num::Wrapping(c(SQRT2INV, 14));
        x6 = (x6 >> 13) * std::num::Wrapping(c(SQRT2INV, 14));
        (x7, x5) = (x7 + x5, x7 - x5);
        (x4, x6) = (x4 + x6, x4 - x6);
        (x4, x7) = inverse_box(
            x4 >> 14,
            x7 >> 14,
            std::num::Wrapping(c(COS3, 12)),
            -std::num::Wrapping(c(SIN3, 12)),
        );
        (x5, x6) = inverse_box(
            x5 >> 14,
            x6 >> 14,
            std::num::Wrapping(c(COS1, 12)),
            -std::num::Wrapping(c(SIN1, 12)),
        );
        (x0, x7) = (x0 + x7, x0 - x7);
        (x1, x6) = (x1 + x6, x1 - x6);
        (x2, x5) = (x2 + x5, x2 - x5);
        (x3, x4) = (x3 + x4, x3 - x4);
        x0 >>= 18;
        x1 >>= 18;
        x2 >>= 18;
        x3 >>= 18;
        x4 >>= 18;
        x5 >>= 18;
        x6 >>= 18;
        x7 >>= 18;
        b[i] = x0.0;
        b[8 + i] = x1.0;
        b[2 * 8 + i] = x2.0;
        b[3 * 8 + i] = x3.0;
        b[4 * 8 + i] = x4.0;
        b[5 * 8 + i] = x5.0;
        b[6 * 8 + i] = x6.0;
        b[7 * 8 + i] = x7.0;
    }
}

/// 逆变换保留 32 位回绕，异常系数不触发调试构建整数溢出。
fn inverse_box(
    x0: std::num::Wrapping<i32>,
    x1: std::num::Wrapping<i32>,
    kcos: std::num::Wrapping<i32>,
    ksin: std::num::Wrapping<i32>,
) -> (std::num::Wrapping<i32>, std::num::Wrapping<i32>) {
    let sum = kcos * (x0 + x1);
    (sum + (ksin - kcos) * x1, sum - (kcos + ksin) * x0)
}
