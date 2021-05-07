use anyhow::{Context, Result};
use clap::{AppSettings, Clap};
use exploring_kafka::set_logger;
use futures::TryStreamExt;
use log::{debug, info, warn};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::Headers,
    ClientConfig, Message,
};

/// This example uses `futures` to process messages in a stream
/// we also set "enable.auto.commit" to `true` so we don't have
/// to manually use `consumer.commit_message`
#[derive(Clap, Debug)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// Topic list
    #[clap(required(true))]
    topics: Vec<String>,
    /// Consumer group
    #[clap(short, long, default_value = "test_group")]
    group: String,
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
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", opts.group)
        .set("bootstrap.servers", opts.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .context("Consumer creation failed")?;

    let topics = opts.topics.iter().map(|o| o.as_str()).collect::<Vec<_>>();
    consumer
        .subscribe(&topics)
        .context("Error subscribing to topics")?;

    let stream_processor = consumer.stream().try_for_each(|msg| async move {
        let payload = match msg.payload_view::<str>() {
            None => "",
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                warn!("Error desearializing message payload: {}", e);
                ""
            }
        };

        info!(
            "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}",
            msg.key(),
            payload,
            msg.topic(),
            msg.partition(),
            msg.offset()
        );

        if let Some(headers) = msg.headers() {
            for i in 0..headers.count() {
                let (k, v) = headers.get(i).unwrap();
                info!("  Header {:#?}: {:?}", k, v);
            }
        }

        Ok(())
    });

    info!("Starting stream processing...");
    stream_processor.await.context("Stream processing failed")?;
    info!("Stream processing terminated");
    Ok(())
}
