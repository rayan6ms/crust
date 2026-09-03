use std::collections::HashMap;
use std::env;
use std::time::Instant;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const QUEUE_CAPACITY: usize = 256;

#[derive(Clone, Copy)]
enum Model {
    Dedicated,
    Sharded,
    Session,
}

impl Model {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "dedicated" => Ok(Self::Dedicated),
            "sharded" => Ok(Self::Sharded),
            "session" => Ok(Self::Session),
            _ => Err(format!("unknown model: {value}")),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Dedicated => "dedicated-player-actor",
            Self::Sharded => "sharded-keyed-executor",
            Self::Session => "session-owned-executor",
        }
    }
}

struct Command {
    guild_id: u64,
    sequence: u64,
    sent: Instant,
    reply: oneshot::Sender<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultRow {
    model: &'static str,
    players: usize,
    worker_tasks: usize,
    rss_delta_kib: u64,
    p50_command_micros: u64,
    p99_command_micros: u64,
    max_command_micros: u64,
    shutdown_micros: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let model = Model::parse(&arguments.next().ok_or("model is required")?)?;
    let players: usize = arguments
        .next()
        .ok_or("player count is required")?
        .parse()?;
    if players == 0 || arguments.next().is_some() {
        return Err("usage: p07_executor_bench MODEL PLAYER_COUNT".into());
    }
    let baseline_rss = rss_kib()?;
    let (workers, replies) = match model {
        Model::Dedicated => dedicated(players).await?,
        Model::Sharded => sharded(players).await?,
        Model::Session => session_owned(players).await?,
    };
    let rss_delta_kib = rss_kib()?.saturating_sub(baseline_rss);
    let worker_tasks = workers.len();
    let mut latencies = Vec::with_capacity(replies.len());
    for reply in replies {
        latencies.push(reply.await?);
    }
    latencies.sort_unstable();
    let shutdown_started = Instant::now();
    for worker in workers {
        worker.await?;
    }
    let result = ResultRow {
        model: model.name(),
        players,
        worker_tasks,
        rss_delta_kib,
        p50_command_micros: percentile(&latencies, 50),
        p99_command_micros: percentile(&latencies, 99),
        max_command_micros: *latencies.last().unwrap_or(&0),
        shutdown_micros: micros(shutdown_started.elapsed()),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn dedicated(
    players: usize,
) -> Result<(Vec<JoinHandle<()>>, Vec<oneshot::Receiver<u64>>), Box<dyn std::error::Error>> {
    let mut workers = Vec::with_capacity(players);
    let mut senders = Vec::with_capacity(players);
    for _ in 0..players {
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        senders.push(sender);
        workers.push(tokio::spawn(async move {
            let mut sequence = 0;
            while let Some(command) = receiver.recv().await {
                sequence = sequence.max(command.sequence);
                std::hint::black_box((command.guild_id, sequence));
                let _ = command.reply.send(micros(command.sent.elapsed()));
            }
        }));
    }
    let mut replies = Vec::with_capacity(players);
    for (guild_id, sender) in senders.iter().enumerate() {
        let (reply, received) = oneshot::channel();
        sender
            .send(Command {
                guild_id: u64::try_from(guild_id)?,
                sequence: 1,
                sent: Instant::now(),
                reply,
            })
            .await?;
        replies.push(received);
    }
    drop(senders);
    Ok((workers, replies))
}

async fn sharded(
    players: usize,
) -> Result<(Vec<JoinHandle<()>>, Vec<oneshot::Receiver<u64>>), Box<dyn std::error::Error>> {
    let shards = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(1, 16);
    keyed(players, shards).await
}

async fn session_owned(
    players: usize,
) -> Result<(Vec<JoinHandle<()>>, Vec<oneshot::Receiver<u64>>), Box<dyn std::error::Error>> {
    keyed(players, 1).await
}

async fn keyed(
    players: usize,
    worker_count: usize,
) -> Result<(Vec<JoinHandle<()>>, Vec<oneshot::Receiver<u64>>), Box<dyn std::error::Error>> {
    let mut workers = Vec::with_capacity(worker_count);
    let mut senders = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (sender, mut receiver) = mpsc::channel::<Command>(QUEUE_CAPACITY);
        senders.push(sender);
        workers.push(tokio::spawn(async move {
            let mut states = HashMap::<u64, u64>::new();
            while let Some(command) = receiver.recv().await {
                let sequence = states.entry(command.guild_id).or_default();
                *sequence = (*sequence).max(command.sequence);
                std::hint::black_box(*sequence);
                let _ = command.reply.send(micros(command.sent.elapsed()));
            }
        }));
    }
    let mut replies = Vec::with_capacity(players);
    for guild_id in 0..players {
        let (reply, received) = oneshot::channel();
        senders[guild_id % worker_count]
            .send(Command {
                guild_id: u64::try_from(guild_id)?,
                sequence: 1,
                sent: Instant::now(),
                reply,
            })
            .await?;
        replies.push(received);
    }
    drop(senders);
    Ok((workers, replies))
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = (values.len() - 1).saturating_mul(percentile) / 100;
    values[index]
}

fn micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn rss_kib() -> std::io::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("missing resident page count"))?
        .parse()
        .map_err(std::io::Error::other)?;
    Ok(resident_pages.saturating_mul(4096) / 1024)
}
