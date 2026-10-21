//! 使用主流 VP8 解码器保留 YCbCr 平面和透明度的完整通道精度。
use image::{DynamicImage, ImageBuffer, Rgba};
use std::io::Cursor;
/// 解码 WebP；有损图像用最近邻色度采样，无损图像由 image 库处理。
pub fn decode(data: &[u8]) -> Result<DynamicImage, String> {
    let ordinary = image::load_from_memory_with_format(data, image::ImageFormat::WebP)
        .map_err(|e| format!("webp: {e}"))?;
    let mut pos = 12;
    while let Some(header) = data.get(pos..pos + 8) {
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let payload = data.get(pos + 8..pos + 8 + len).ok_or("unexpected EOF")?;
        if &header[..4] == b"VP8 " {
            let frame = image_webp::vp8::Vp8Decoder::decode_frame(Cursor::new(payload))
                .map_err(|e| format!("webp: {e}"))?;
            let stride = (frame.width as usize).div_ceil(16) * 16;
            let alpha = ordinary.to_rgba8();
            let mut out = ImageBuffer::new(frame.width as u32, frame.height as u32);
            for (x, y, p) in out.enumerate_pixels_mut() {
                let i = y as usize * stride + x as usize;
                let ci = (y as usize / 2) * (stride / 2) + x as usize / 2;
                let channels =
                    super::jpeg_decode::ycbcr(frame.ybuf[i], frame.ubuf[ci], frame.vbuf[ci]);
                *p = Rgba([
                    channels[0],
                    channels[1],
                    channels[2],
                    alpha.get_pixel(x, y)[3] as u16 * 257,
                ]);
            }
            return Ok(DynamicImage::ImageRgba16(out));
        }
        pos += 8 + len + len % 2;
    }
    Ok(ordinary)
}
