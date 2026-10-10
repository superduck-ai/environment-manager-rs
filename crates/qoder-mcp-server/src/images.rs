//! 图片格式检测、尺寸预算、缩放及 MCP 图片内容块。
use super::{ContentBlock, ToolResult};
use crate::util;
use base64::{Engine, engine::general_purpose::STANDARD};
use image::{DynamicImage, ImageFormat, ImageReader};
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};
const BUDGET: usize = 5 << 20;
/// 根据实际解码格式确定 MIME 类型。
fn media(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "image/png",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Gif => "image/gif",
        ImageFormat::WebP => "image/webp",
        _ => "application/octet-stream",
    }
}
/// 以二进制单位输出图片文件大小。
fn size(n: usize) -> String {
    if n < 1024 {
        return format!("{n} B");
    }
    let mut x = n as f64;
    for unit in ["KiB", "MiB", "GiB"] {
        x /= 1024.0;
        if x < 1024.0 {
            return format!("{x:.1} {unit}");
        }
    }
    format!("{:.1} TiB", x / 1024.0)
}
/// 判断编码后 base64 大小是否满足预算。
fn fits(n: usize) -> bool {
    n.div_ceil(3) * 4 <= BUDGET
}
/// 读取图片，小图片逐字保留原数据，大图片逐级缩放与重编码。
pub fn read(path: &Path) -> ToolResult {
    let info = match fs::metadata(path) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return ToolResult::error(format!("Image file not found: {}", path.display()));
        }
        Err(e) => {
            return ToolResult::error(format!(
                "failed to stat image file: {}",
                util::io_error("stat", path, &e)
            ));
        }
    };
    if info.is_dir() {
        return ToolResult::error(format!(
            "{} is a directory, not an image file.",
            path.display()
        ));
    }
    let reader = match fs::File::open(path).map(|f| ImageReader::new(std::io::BufReader::new(f))) {
        Ok(r) => r,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to prepare image file: open image file: {}",
                util::io_error("open", path, &e)
            ));
        }
    };
    let mut reader = match reader.with_guessed_format() {
        Ok(r) => r,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to prepare image file: invalid or unsupported image data: {e}"
            ));
        }
    };
    reader.no_limits();
    let Some(format) = reader.format() else {
        return ToolResult::error(
            "failed to prepare image file: invalid or unsupported image data: image: unknown format",
        );
    };
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::Gif | ImageFormat::WebP
    ) {
        return ToolResult::error(
            "failed to prepare image file: invalid or unsupported image data: image: unknown format",
        );
    }
    let mut header = [0u8; 33];
    let mut input = reader.into_inner();
    let bytes = match input.read(&mut header) {
        Ok(bytes) => bytes,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to prepare image file: invalid or unsupported image data: {}",
                util::io_error("read", path, &e)
            ));
        }
    };
    if let Err(e) = input.seek(SeekFrom::Start(0)) {
        return ToolResult::error(format!(
            "failed to prepare image file: invalid or unsupported image data: {}",
            util::io_error("seek", path, &e)
        ));
    }
    let mut reader = ImageReader::with_format(input, format);
    reader.no_limits();
    if format == ImageFormat::Png && bytes >= 29 {
        let width = i32::from_be_bytes(header[16..20].try_into().unwrap());
        let height = i32::from_be_bytes(header[20..24].try_into().unwrap());
        let error = if width <= 0 || height <= 0 {
            Some("png: invalid format: non-positive dimension")
        } else if width as i64 * height as i64 > i64::MAX / 8 {
            Some("png: unsupported feature: dimension overflow")
        } else {
            None
        };
        if let Some(error) = error {
            return ToolResult::error(format!(
                "failed to prepare image file: invalid or unsupported image data: {error}"
            ));
        }
    }
    if format == ImageFormat::Gif {
        let error = if bytes < 13 {
            Some("gif: reading header: unexpected EOF")
        } else if header[10] & 128 != 0 && info.len() < 13 + 3 * (1 << ((header[10] & 7) + 1)) {
            Some("gif: reading color table: unexpected EOF")
        } else {
            None
        };
        if let Some(error) = error {
            return ToolResult::error(format!(
                "failed to prepare image file: invalid or unsupported image data: {error}"
            ));
        }
    }
    let (w, h) = match reader.into_dimensions() {
        Ok(v) => v,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to prepare image file: invalid or unsupported image data: {}",
                decode_error(format, &e)
            ));
        }
    };
    if w == 0 || h == 0 {
        return ToolResult::error(format!(
            "failed to prepare image file: invalid image dimensions: {w}x{h}"
        ));
    }
    if w as u64 * h as u64 > 50_000_000 {
        return ToolResult::error(format!(
            "failed to prepare image file: image dimensions too large: {w}x{h} (max 50000000 pixels)"
        ));
    }
    let raw = match fs::read(path) {
        Ok(v) => v,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to prepare image file: read image file: {}",
                util::io_error("open", path, &e)
            ));
        }
    };
    let (data, mime, tw, th, reencoded) = if fits(raw.len()) && w <= 2000 && h <= 2000 {
        (raw, media(format), w, h, false)
    } else {
        let decoded = match if format == ImageFormat::Jpeg {
            super::jpeg_decode::decode(&raw)
        } else if format == ImageFormat::Gif {
            super::gif_decode::decode(&raw)
        } else if format == ImageFormat::WebP {
            super::webp_decode::decode(&raw)
        } else {
            image::load_from_memory_with_format(&raw, format).map_err(|e| decode_error(format, &e))
        } {
            Ok(v) => v,
            Err(e) => {
                return ToolResult::error(format!(
                    "failed to prepare image file: decode image for resizing: {}",
                    e
                ));
            }
        };
        match prepare(&decoded, format) {
            Ok((data, mime, tw, th)) => (data, mime, tw, th, true),
            Err(e) => return ToolResult::error(format!("failed to prepare image file: {e}")),
        }
    };
    let resized = tw != w || th != h;
    let action = if resized && reencoded {
        "resized and re-encoded"
    } else if resized {
        "resized"
    } else if reencoded {
        "re-encoded"
    } else {
        "kept original"
    };
    let ext = path
        .extension()
        .map_or(String::new(), |e| format!(".{}", e.to_string_lossy()));
    let mut summary = format!(
        "Read image: {}\nOriginal: {w}x{h}, {}, {ext}\nReturned: {tw}x{th}, {}, {mime}, {action}",
        path.display(),
        size(info.len() as usize),
        size(data.len())
    );
    if resized {
        summary.push_str(&format!("\nCoordinate mapping: multiply returned x by {:.4} and y by {:.4} to map to the original image.",w as f64/tw as f64,h as f64/th as f64));
    }
    let mut result = ToolResult::text(summary);
    result.content.push(ContentBlock {
        kind: "image".into(),
        text: String::new(),
        data: STANDARD.encode(data),
        mime_type: mime.into(),
    });
    result
}
/// 将常见解码错误映射为参考图片库的文本。
fn decode_error(format: ImageFormat, e: &image::ImageError) -> String {
    match format {
        ImageFormat::Png => {
            if e.to_string().contains("unexpected") || e.to_string().contains("EOF") {
                "unexpected EOF".into()
            } else {
                format!("png: {e}")
            }
        }
        ImageFormat::Jpeg => {
            if e.to_string().contains("Not enough bytes") || e.to_string().contains("EOF") {
                "unexpected EOF".into()
            } else {
                format!("invalid JPEG format: {e}")
            }
        }
        ImageFormat::Gif => format!("gif: {e}"),
        ImageFormat::WebP => format!("webp: {e}"),
        _ => "image: unknown format".into(),
    }
}
/// 逐个尺寸尝试 PNG 和不同 JPEG 质量等级。
fn prepare(
    image: &DynamicImage,
    format: ImageFormat,
) -> Result<(Vec<u8>, &'static str, u32, u32), String> {
    let (w, h) = (image.width(), image.height());
    let max = w.max(h);
    let mut edges = Vec::new();
    for edge in [2000.min(max), 1600, 1200, 1000, 800, 600, 480, 320] {
        if edge > 0 && edge <= max && !edges.contains(&edge) {
            edges.push(edge);
        }
    }
    for edge in edges {
        let (tw, th) = if max <= edge {
            (w, h)
        } else {
            let scale = edge as f64 / max as f64;
            (
                (w as f64 * scale).round().max(1.0) as u32,
                (h as f64 * scale).round().max(1.0) as u32,
            )
        };
        let rgba = super::image_resize::resize(image, tw, th);
        if format == ImageFormat::Png {
            let data = super::png_encode::encode(&rgba);
            if fits(data.len()) {
                return Ok((data, "image/png", tw, th));
            }
        }
        let mut rgb = image::RgbImage::new(tw, th);
        for (x, y, p) in rgba.enumerate_pixels() {
            let alpha = p[3] as u32;
            let blend = |c: u8| c.saturating_add((255 - alpha) as u8);
            rgb.put_pixel(x, y, image::Rgb([blend(p[0]), blend(p[1]), blend(p[2])]));
        }
        for quality in [85, 75, 65, 55, 45, 35, 25] {
            let data = super::jpeg_encode::encode(&rgb, quality);
            if fits(data.len()) {
                return Ok((data, "image/jpeg", tw, th));
            }
        }
    }
    Err(format!(
        "image could not be reduced below {} encoded budget",
        size(BUDGET)
    ))
}
