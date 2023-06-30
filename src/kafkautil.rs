use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer};
use rdkafka::util::{Timeout};
use std::time::{Duration};

pub fn get_topic_partition_count(config:&mut ClientConfig, topic_name: &str) ->Option<i32> {
    let consumer: BaseConsumer = config
    .create()
    .expect("Consumer creation error");
    
    let topic_meta = consumer
        .fetch_metadata(Some(topic_name), Timeout::After(Duration::from_secs(5)))
        .expect("Failed to retrieve topic metadata after 20 seconds");

    if topic_meta.topics().len()== 0 {
        panic!("Topic {topic_name} not found");
    }

    if let Some(topic_data) = topic_meta.topics().first() {
        if topic_data.partitions().len() > 0 {
            let partition_count = topic_data.partitions().last().unwrap().id() + 1;
            return Some(partition_count);
        }
    }

    return None
}