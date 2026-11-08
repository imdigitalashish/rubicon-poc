use rand::Rng;
use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};

/// Simulated network or storage latency with ±20% jitter.
pub async fn delay(d: Duration) {
    if d.is_zero() {
        return;
    }
    let d = d.mul_f64(rand::thread_rng().gen_range(0.8..1.25));
    tokio::time::sleep(d).await;
}

/// Milliseconds since the process started; the clock used for leases.
pub fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}
