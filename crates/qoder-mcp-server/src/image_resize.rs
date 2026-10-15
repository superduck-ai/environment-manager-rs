//! 与参考图像缩放器一致的 Catmull-Rom 权重、预乘透明度和取整。
use image::{DynamicImage, RgbaImage};
struct Distribution {
    weights: Vec<(usize, f64)>,
    inverse: f64,
}
/// 计算一维目标像素的源像素权重，缩小时扩展采样支撑宽度。
fn distribution(dst: usize, src: usize) -> Vec<Distribution> {
    let scale = src as f64 / dst as f64;
    let support = 2.0 * scale.max(1.0);
    let arg = if scale > 1.0 { 1.0 / scale } else { 1.0 };
    (0..dst)
        .map(|x| {
            let center = (x as f64 + 0.5) * scale - 0.5;
            let first = (center - support).floor().max(0.0) as usize;
            let last = ((center + support).ceil() as usize).min(src);
            let mut weights = Vec::new();
            let mut total = 0.0;
            for i in first..last {
                let t = ((center - i as f64) * arg).abs();
                let w = if t >= 2.0 {
                    0.0
                } else if t < 1.0 {
                    (1.5 * t - 2.5) * t * t + 1.0
                } else {
                    ((-0.5 * t + 2.5) * t - 4.0) * t + 2.0
                };
                if w != 0.0 {
                    total += w;
                    weights.push((i, w));
                }
            }
            Distribution {
                weights,
                inverse: 1.0 / total,
            }
        })
        .collect()
}
/// 将归一化预乘通道取整到参考实现的 16 位再取高字节。
fn byte(value: f64) -> u8 {
    ((65535.0 * value + 0.5) as i32)
        .clamp(0, 65535)
        .wrapping_shr(8) as u8
}
/// 返回预乘 RGBA 字节；两个方向分别累积浮点权重。
pub fn resize(src: &DynamicImage, width: u32, height: u32) -> RgbaImage {
    let (w, h) = (src.width() as usize, src.height() as usize);
    let (dw, dh) = (width as usize, height as usize);
    let rgba = src.to_rgba16();
    let mut out = RgbaImage::new(width, height);
    if (dw, dh) == (w, h) {
        for (x, y, p) in rgba.enumerate_pixels() {
            let a = p[3] as u32;
            out.put_pixel(
                x,
                y,
                image::Rgba([
                    ((p[0] as u32 * a / 65535) >> 8) as u8,
                    ((p[1] as u32 * a / 65535) >> 8) as u8,
                    ((p[2] as u32 * a / 65535) >> 8) as u8,
                    (a >> 8) as u8,
                ]),
            );
        }
        return out;
    }
    let horizontal = distribution(dw, w);
    let vertical = distribution(dh, h);
    let mut tmp = vec![[0.0; 4]; dw * h];
    for y in 0..h {
        for (x, d) in horizontal.iter().enumerate() {
            let mut sum = [0.0; 4];
            for (i, weight) in &d.weights {
                let p = rgba.get_pixel(*i as u32, y as u32);
                let a = p[3] as u32;
                for c in 0..3 {
                    sum[c] += (p[c] as u32 * a / 65535) as f64 * weight;
                }
                sum[3] += a as f64 * weight;
            }
            for c in 0..4 {
                tmp[y * dw + x][c] = sum[c] * (d.inverse / 65535.0);
            }
        }
    }
    for x in 0..dw {
        for (y, d) in vertical.iter().enumerate() {
            let mut sum = [0.0; 4];
            for (i, weight) in &d.weights {
                for c in 0..4 {
                    sum[c] += tmp[i * dw + x][c] * weight;
                }
            }
            for c in 0..3 {
                sum[c] = sum[c].min(sum[3]);
            }
            out.put_pixel(
                x as u32,
                y as u32,
                image::Rgba(sum.map(|v| byte(v * d.inverse))),
            );
        }
    }
    out
}
/// PNG 使用非预乘像素，反变换采用参考编码器的整数公式。
pub fn unpremultiply(src: &RgbaImage) -> RgbaImage {
    let mut out = src.clone();
    for p in out.pixels_mut() {
        let a = p[3] as u32;
        if a == 0 {
            *p = image::Rgba([0; 4]);
        } else if a != 255 {
            for c in 0..3 {
                p[c] = ((p[c] as u32 * 65535 / a) >> 8) as u8;
            }
        }
    }
    out
}
