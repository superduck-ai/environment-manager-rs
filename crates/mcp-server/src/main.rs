//! 独立 MCP 可执行入口，兼容单横线和双横线参数。
use mcp_server::{
    Config, Registry, Server,
    environment::{EnvironmentProvider, UnixSocketProvider},
    plugins::Manager,
};
use std::{path::PathBuf, sync::Arc};

/// 解析参考服务参数并运行相应传输。
#[tokio::main]
async fn main() {
    let mut transport = "stdio".to_owned();
    let mut addr = ":3001".to_owned();
    let mut workdir = "/data".to_owned();
    let mut socket = String::new();
    let mut core = String::new();
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--" || !argument.starts_with('-') {
            break;
        }
        let (key, inline) = argument
            .trim_start_matches('-')
            .split_once('=')
            .map_or((argument.trim_start_matches('-'), None), |(k, v)| {
                (k, Some(v.to_owned()))
            });
        let target = match key {
            "transport" => &mut transport,
            "addr" => &mut addr,
            "workdir" => &mut workdir,
            "env-socket" => &mut socket,
            "core-addr" => &mut core,
            "h" | "help" => {
                print_help();
                return;
            }
            _ => {
                eprintln!("flag provided but not defined: -{key}");
                print_help();
                std::process::exit(2);
            }
        };
        *target = inline.unwrap_or_else(|| {
            args.next().unwrap_or_else(|| {
                eprintln!("flag needs an argument: -{key}");
                print_help();
                std::process::exit(2);
            })
        });
    }
    if let Ok(value) = std::env::var("WORK_DIR")
        && !value.is_empty()
    {
        workdir = value;
    }
    let environment: Option<Arc<dyn EnvironmentProvider>> = if socket.is_empty() {
        None
    } else {
        mcp_server::logging::info(
            "runtime environment provider enabled",
            &[("source", "core_socket")],
        );
        Some(Arc::new(UnixSocketProvider::new(socket)))
    };
    let registry = Arc::new(Registry::builtin(PathBuf::from(workdir), environment));
    let plugins = Arc::new(if core.is_empty() {
        Manager::default()
    } else {
        mcp_server::logging::info("plugin service management enabled", &[("core_addr", &core)]);
        Manager::with_service(&core)
    });
    let mut config = Config::default();
    if let Ok(hosts) = std::env::var("MCP_ALLOWED_HOSTS") {
        config.allowed_hosts = hosts
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
    }
    if let Ok(origins) = std::env::var("MCP_ALLOWED_ORIGINS") {
        config.allowed_origins = origins
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
    }
    let server = Arc::new(Server::new(registry, plugins.clone(), config));
    let result = match transport.as_str() {
        "stdio" => server.serve_stdio().await,
        "http" => server.serve_http(&addr).await,
        _ => {
            mcp_server::logging::error("unsupported transport", &[("transport", &transport)]);
            std::process::exit(1);
        }
    };
    plugins.shutdown_all().await;
    if let Err(error) = result {
        mcp_server::logging::error("server error", &[("error", &error.to_string())]);
        std::process::exit(1);
    }
}
/// 打印与参考服务一致的参数帮助。
fn print_help() {
    eprint!(
        "Usage of {}:\n  -addr string\n    \tHTTP listen address (only used with -transport=http) (default \":3001\")\n  -core-addr string\n    \tsandbox-runtime-core address for plugin service management (e.g. localhost:49983)\n  -env-socket string\n    \tUnix socket providing the current envd environment for child processes\n  -transport string\n    \tTransport type: stdio or http (default \"stdio\")\n  -workdir string\n    \tWorking directory for tool execution (default \"/data\")\n",
        std::env::args().next().unwrap_or_default()
    );
}
