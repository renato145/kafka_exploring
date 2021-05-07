use anyhow::{Context, Result};
use clap::{AppSettings, Clap};
use exploring_kafka::set_logger;
use log::{debug, info, warn};
use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::Headers,
    ClientConfig, ClientContext, Message, TopicPartitionList,
};

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

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    set_logger(opts.verbose)?;
    debug!("{:#?}", opts);

    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", opts.group)
        .set("bootstrap.servers", opts.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .create_with_context(context)
        .context("Consumer creation failed")?;

    let topics = opts.topics.iter().map(|o| o.as_str()).collect::<Vec<_>>();
    consumer
        .subscribe(&topics)
        .context("Error subscribing to topics")?;

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(msg) => {
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

                consumer
                    .commit_message(&msg, CommitMode::Async)
                    .context("Error commiting message")?;
            }
        }
    }
}
