use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub tcp: TcpConfig,
    pub websocket: WebSocketConfig,
    pub timer: TimerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub name: String,
    pub log_level: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TcpConfig {
    pub enabled: bool,
    pub addr: String,
    pub max_connections: usize,
    pub read_buffer_size: usize,
    pub write_buffer_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub addr: String,
    pub path: String,
    pub max_connections: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TimerConfig {
    pub tick_interval_ms: u64,
    pub max_timers: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
}

impl AppConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn default_config() -> Self {
        AppConfig {
            server: ServerConfig {
                name: "shell-server".to_string(),
                log_level: "debug".to_string(),
            },
            tcp: TcpConfig {
                enabled: true,
                addr: "0.0.0.0:9527".into(),
                max_connections: 10000,
                read_buffer_size: 8192,
                write_buffer_size: 8192,
            },
            websocket: WebSocketConfig {
                enabled: true,
                addr: "0.0.0.0:9528".to_string(),
                path: "/ws".to_string(),
                max_connections: 10000,
            },
            timer: TimerConfig {
                tick_interval_ms: 100,
                max_timers: 100000,
            },
        }
    }
}
