use mcp_server::Registry;
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn read_limits_default_lines_and_allows_explicit_continuation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lines.txt");
    let text = (0..2001)
        .map(|index| format!("row{index}\n"))
        .collect::<String>();
    tokio::fs::write(&path, text).await.unwrap();
    let registry = Registry::builtin(directory.path().to_path_buf(), None);
    let read = registry.get("read").unwrap();
    let result = read
        .handle(CancellationToken::new(), json!({"file_path":path}))
        .await
        .unwrap();
    assert!(result.content[0].text.contains("2000\trow1999"));
    assert!(!result.content[0].text.contains("2001\trow2000"));
    assert!(result.content[0].text.contains("start_line/end_line"));
    let result = read
        .handle(
            CancellationToken::new(),
            json!({"file_path":path,"start_line":2000,"end_line":2001}),
        )
        .await
        .unwrap();
    assert_eq!(result.content[0].text, "2001\trow2000");
}
