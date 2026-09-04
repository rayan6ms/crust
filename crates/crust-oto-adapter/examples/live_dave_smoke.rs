//! Manual, credential-safe live check for the Crust-to-Oto connection boundary.

use std::error::Error;
use std::io::{self, Read};
use std::time::Duration;

use crust::voice::{VoiceBackend, VoiceConnectionInfo, VoicePhase, VoiceSecret};
use crust_oto_adapter::OtoVoiceBackend;
use serde_json::Value;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DAVE_TIMEOUT: Duration = Duration::from_secs(30);

fn string_field(value: &Value, field: &str) -> Result<String, Box<dyn Error>> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("{field} must be a string").into())
}

fn snowflake_field(value: &Value, field: &str) -> Result<u64, Box<dyn Error>> {
    string_field(value, field)?
        .parse()
        .map_err(|_| format!("{field} must be an unsigned 64-bit Discord snowflake").into())
}

fn read_voice_info() -> Result<VoiceConnectionInfo, Box<dyn Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let value: Value = serde_json::from_str(&input)?;
    Ok(VoiceConnectionInfo {
        guild_id: snowflake_field(&value, "server_id")?,
        user_id: snowflake_field(&value, "user_id")?,
        channel_id: snowflake_field(&value, "channel_id")?,
        endpoint: string_field(&value, "endpoint")?,
        session_id: VoiceSecret::new(string_field(&value, "session_id")?),
        token: VoiceSecret::new(string_field(&value, "token")?),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let info = read_voice_info()?;
    let backend = OtoVoiceBackend::with_defaults(1, 1)?;
    let connection = timeout(
        CONNECT_TIMEOUT,
        backend.connect(info, CancellationToken::new()),
    )
    .await
    .map_err(|_| "Crust-to-Oto connection timed out")??;
    let ready = timeout(DAVE_TIMEOUT, async {
        loop {
            let snapshot = connection.snapshot().await?;
            if matches!(snapshot.phase, VoicePhase::Connected | VoicePhase::Closed) {
                return Ok::<_, crust::voice::VoiceError>(snapshot);
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let result = match ready {
        Ok(Ok(snapshot)) if snapshot.phase == VoicePhase::Connected => {
            println!("connected: phase={:?}", snapshot.phase);
            Ok(())
        }
        Ok(Ok(snapshot)) => Err(format!(
            "Crust-to-Oto connection closed before DAVE readiness: {:?}",
            snapshot.phase
        )
        .into()),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            let phase = connection.snapshot().await?.phase;
            Err(format!("Crust-to-Oto DAVE setup timed out in phase {phase:?}").into())
        }
    };
    connection.shutdown().await?;
    backend.shutdown().await?;
    result
}
