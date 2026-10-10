//! 读取 GIF 的第一个原始帧，保持参考解码器对子帧边界的处理。
use image::{DynamicImage, RgbaImage};
use std::io::Cursor;
/// 通过主流 GIF 解码器提取首帧像素，不扩展到动画画布。
pub fn decode(data: &[u8]) -> Result<DynamicImage, String> {
    let mut options = gif::DecodeOptions::new();
    options.set_color_output(gif::ColorOutput::RGBA);
    let mut decoder = options
        .read_info(Cursor::new(data))
        .map_err(|e| format!("gif: {e}"))?;
    let frame = decoder
        .read_next_frame()
        .map_err(|e| format!("gif: {e}"))?
        .ok_or("gif: missing image data")?;
    let image = RgbaImage::from_raw(
        frame.width as u32,
        frame.height as u32,
        frame.buffer.to_vec(),
    )
    .ok_or("gif: invalid image data")?;
    Ok(DynamicImage::ImageRgba8(image))
}
