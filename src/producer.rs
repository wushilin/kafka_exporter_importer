use std::time::Duration;

use log::info;
pub mod cliutil;
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

mod logutil;
use logutil::setup_logger;

#[derive(Parser, Debug, Clone)]
pub struct CliArg {
    #[arg(short, long, default_value_t=String::from("client.properties"))]
    pub command_config: String,
    #[arg(short, long)]
    pub topic: String,
    #[arg(short, long, default_value_t=String::from("rdkafka=trace"))]
    pub log_conf: String,
}
#[tokio::main]
async fn main() {
    let args = CliArg::parse();
    let command_config = args.command_config;
    let topic = args.topic;
    let log_conf = args.log_conf;
    setup_logger(true, Some(log_conf.as_str()));
    let mut config = cliutil::load_config(&command_config).expect("Config can't be loaded");
    println!("Loaded config: {config:#?}");
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    produce(&mut config, &topic).await;
}


async fn produce(config: &mut ClientConfig, topic_name: &String) {
    let producer: &FutureProducer = &config
    .create()
    .expect("Producer creation error");
    let futures = (0..5)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header { key: "SomeHeaderName", value: Some("SomeHeaderValue") })),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}