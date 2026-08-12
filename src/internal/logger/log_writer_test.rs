//! `LogWriter` 的行缓冲行为测试。

use std::{
    fmt,
    io::Write,
    sync::{Arc, Mutex},
};

use tracing::{Event, Level, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt as _};

use super::{LogWriter, MAX_LOG_WRITER_BUFFERED_LINE};

#[derive(Clone, Default)]
struct CaptureLayer(Arc<Mutex<Vec<(Level, String)>>>);

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _context: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        self.0.lock().unwrap().push((
            *event.metadata().level(),
            visitor.message.unwrap_or_default(),
        ));
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}

#[test]
fn writes_complete_lines() {
    let capture = CaptureLayer::default();
    let records = Arc::clone(&capture.0);
    let subscriber = tracing_subscriber::registry().with(capture);

    tracing::subscriber::with_default(subscriber, || {
        let mut writer = LogWriter::new(Level::INFO, "cc: ");
        assert_eq!(writer.write(b"first\nsecond").unwrap(), 12);
        assert_eq!(records.lock().unwrap().len(), 1);
        assert_eq!(writer.write(b" line\n").unwrap(), 6);
    });

    assert_eq!(
        *records.lock().unwrap(),
        vec![
            (Level::INFO, "cc: first".to_owned()),
            (Level::INFO, "cc: second line".to_owned()),
        ]
    );
}

#[test]
fn truncates_overlong_buffered_line() {
    let capture = CaptureLayer::default();
    let records = Arc::clone(&capture.0);
    let subscriber = tracing_subscriber::registry().with(capture);
    let payload = vec![b'x'; MAX_LOG_WRITER_BUFFERED_LINE + 1];

    tracing::subscriber::with_default(subscriber, || {
        let mut writer = LogWriter::new(Level::WARN, "cc: ");
        assert_eq!(writer.write(&payload).unwrap(), payload.len());
        assert_eq!(writer.buffered_len(), 0);
    });

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, Level::WARN);
    let suffix = " [line truncated - no newline]";
    assert!(records[0].1.starts_with("cc: "));
    assert!(records[0].1.ends_with(suffix));
    assert_eq!(
        records[0].1.len(),
        "cc: ".len() + payload.len() + suffix.len()
    );
}
