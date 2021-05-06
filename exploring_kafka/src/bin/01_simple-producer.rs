use anyhow::{Context, Result};
use clap::{AppSettings, Clap};
use log::{info, set_max_level, LevelFilter};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use simple_logger::SimpleLogger;
use std::time::Duration;

#[derive(Clap, Debug)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// Message to send
    msg: String,
    /// Destination topic
    #[clap(short, long)]
    topic: String,
    /// Broker list in kafka format
    #[clap(short, long, default_value = "localhost:9092")]
    brokers: String,
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    SimpleLogger::new().init()?;
    let log_level = match opts.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    set_max_level(log_level);
    info!("{:#?}", opts);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", opts.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("Failed to create producer")?;

    let record = FutureRecord::to(&opts.topic).payload(&opts.msg).key("");
    let delivery_status = producer.send(record, Duration::from_secs(0)).await;
    let a = delivery_status.unwrap();
    println!("{:#?}", a);
    info!("Message received");

    Ok(())
}
