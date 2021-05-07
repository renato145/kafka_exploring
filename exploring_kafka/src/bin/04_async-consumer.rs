use std::time::Duration;

use anyhow::{Context, Result};
use clap::{AppSettings, Clap};
use exploring_kafka::set_logger;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use log::{debug, info, warn};
use rdkafka::{
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::Headers,
    ClientConfig, ClientContext, Message, TopicPartitionList,
};

#[derive(Clap, Debug, Clone)]
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
    /// Number of workers
    #[clap(short, long, default_value = "1")]
    workers: usize,
    /// Log level -> default: info, -v: debug, -vv trace
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
}

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext {
    worker: usize,
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance (worker {}) {:?}", self.worker, rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance (worker {}) {:?}", self.worker, rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets (worker {}): {:?}", self.worker, result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    set_logger(opts.verbose)?;
    debug!("{:#?}", opts);

    info!("Launching {} workers...", opts.workers);

    (0..opts.workers)
        .map(|i| tokio::spawn(run_async_processor(i, opts.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await;

    Ok(())
}

async fn run_async_processor(worker: usize, opts: Opts) -> Result<()> {
    let context = CustomContext { worker };
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &opts.group)
        .set("bootstrap.servers", &opts.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create_with_context(context)
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

        info!("Processing message (worker {})...", worker);
        tokio::time::sleep(Duration::from_secs(2)).await;

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

    info!("Starting stream processing (worker {})...", worker);
    stream_processor.await.context("Stream processing failed")?;
    info!("Stream processing terminated (worker {})", worker);
    Ok(())
}
