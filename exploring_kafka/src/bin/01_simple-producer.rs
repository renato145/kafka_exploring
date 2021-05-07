use anyhow::{Context, Result};
use clap::{AppSettings, Clap};
use exploring_kafka::set_logger;
use log::{debug, info};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use std::time::Duration;

#[derive(Clap, Debug)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// Message to send
    msg: String,
    /// Destination topic
    #[clap(short, long)]
    topic: String,
    /// Key is used when you want messages to go to the same partition
    #[clap(short, long)]
    key: Option<String>,
    /// Broker list in kafka format
    #[clap(short, long, default_value = "localhost:9092")]
    brokers: String,
    /// Log level -> default: info, -v: debug, -vv trace
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    set_logger(opts.verbose)?;
    debug!("{:#?}", opts);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", opts.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("Failed to create producer")?;

    let key = opts.key.as_ref().map_or("", |o| o.as_str());
    let record = FutureRecord::to(&opts.topic).payload(&opts.msg).key(key);
    let (partition, offset) = producer
        .send(record, Duration::from_secs(0))
        .await
        .expect("Error sending the message");

    info!(
        "Message received (partition={}, offset={})",
        partition, offset
    );
    Ok(())
}
