use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

use crate::error::{ShellError, ShellResult};

static TIMER_ID_GEN: AtomicU64 = AtomicU64::new(1);

fn next_timer_id() -> u64 {
    TIMER_ID_GEN.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
struct TimerEntry {
    id: u64,
    fire_at: Instant,
    interval: Option<Duration>, // None = one-shot, Some = repeating
    actor_name: String,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.fire_at.cmp(&other.fire_at)
    }
}

#[derive(Debug, Clone)]
pub struct TimerEvent {
    pub timer_id: u64,
    pub actor_name: String,
}

pub struct TimerService {
    event_tx: mpsc::Sender<TimerEvent>,
    cmd_tx: mpsc::Sender<TimerCommand>,
}

enum TimerCommand {
    Add(TimerEntry),
    Cancel(u64),
    Stop,
}

impl TimerService {
    pub fn new(tick_interval_ms: u64) -> (Self, mpsc::Receiver<TimerEvent>) {
        let (event_tx, event_rx) = mpsc::channel(4096);
        let (cmd_tx, cmd_rx) = mpsc::channel(4096);

        let service = TimerService { event_tx, cmd_tx };

        let event_tx_clone = service.event_tx.clone();
        let tick = Duration::from_millis(tick_interval_ms);

        tokio::spawn(async move { Self::timer_loop(cmd_rx, event_tx_clone, tick).await });

        (service, event_rx)
    }

    async fn timer_loop(
        mut cmd_rx: mpsc::Receiver<TimerCommand>,
        event_tx: mpsc::Sender<TimerEvent>,
        tick: Duration,
    ) {
        let mut heap: BinaryHeap<Reverse<TimerEntry>> = BinaryHeap::new();
        let mut cancelled: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut interval = tokio::time::interval(tick);

        tracing::info!("[TimerService] started with tick interval {:?}", tick);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    while let Some(Reverse(entry)) = heap.peek() {
                        if entry.fire_at > now {
                            break;
                        }

                        let entry = heap.pop().unwrap().0;

                        if cancelled.remove(&entry.id) {
                            continue;
                        }

                        let _ = event_tx.send(TimerEvent {
                            timer_id: entry.id,
                            actor_name: entry.actor_name.clone(),
                        }).await;

                        // If repeating, re-add
                        if let Some(interval_dur) = entry.interval {
                            let new_entry = TimerEntry {
                                fire_at: now + interval_dur,
                                ..entry
                            };
                            heap.push(Reverse(new_entry));
                        }
                    }
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        TimerCommand::Add(entry) => {
                            heap.push(Reverse(entry));
                        }
                        TimerCommand::Cancel(id) => {
                            cancelled.insert(id);
                        }
                        TimerCommand::Stop => {
                            tracing::info!("[TimerService] stopped");
                            break;
                        }
                    }
                }
            }
        }
    }

    pub async fn add_timer(
        &self,
        actor_name: &str,
        delay: Duration,
        is_interval: bool,
    ) -> ShellResult<u64> {
        let id = next_timer_id();
        let interval = if is_interval { Some(delay) } else { None };

        let entry = TimerEntry {
            id,
            fire_at: Instant::now() + delay,
            interval,
            actor_name: actor_name.to_string(),
        };

        self.cmd_tx
            .send(TimerCommand::Add(entry))
            .await
            .map_err(|_| ShellError::Timer("Failed to add timer".to_string()))?;

        Ok(id)
    }

    pub async fn cancel_timer(&self, timer_id: u64) -> ShellResult<()> {
        self.cmd_tx
            .send(TimerCommand::Cancel(timer_id))
            .await
            .map_err(|_| ShellError::Timer("Failed to cancel timer".to_string()))
    }

    pub async fn stop(&self) -> ShellResult<()> {
        self.cmd_tx
            .send(TimerCommand::Stop)
            .await
            .map_err(|_| ShellError::Timer("Failed to stop timer".to_string()))
    }
}

impl Clone for TimerService {
    fn clone(&self) -> Self {
        TimerService {
            event_tx: self.event_tx.clone(),
            cmd_tx: self.cmd_tx.clone(),
        }
    }
}
