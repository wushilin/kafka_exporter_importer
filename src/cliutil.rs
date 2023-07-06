use fs::File;
use java_properties::read;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::fs;
use std::io::BufReader;

pub fn load_properties(
    filename: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let result = read(BufReader::new(f))?;
    return Ok(result);
}

pub fn load_config(filename: &str) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let map = load_properties(filename)?;
    let mut result = ClientConfig::new();
    result.extend(map);
    result.log_level = RDKafkaLogLevel::Warning;
    return Ok(result);
}

pub fn properties_to_config(map: &HashMap<String, String>) -> ClientConfig {
    let mut result = ClientConfig::new();
    let mut map_new = HashMap::new();
    let prefix = "kafka.";
    for (k, v) in map {
        if k.starts_with(prefix) {
            let kc = k.clone();
            let start_index = prefix.len();
            let end_index = kc.len();
            let substr = kc.get(start_index..end_index).unwrap().to_string();
            map_new.insert(substr, v.clone());
        }
    }
    result.extend(map_new);
    result.log_level = RDKafkaLogLevel::Warning;
    return result;
}