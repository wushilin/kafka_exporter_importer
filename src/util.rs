use base64::{engine::general_purpose, Engine as _};
use log::info;

pub fn base64_encode(data: &[u8]) -> String {
    return general_purpose::STANDARD_NO_PAD.encode(data);
}

pub fn base64_decode(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    return general_purpose::STANDARD_NO_PAD.decode(input);
}

pub fn time(tag: &str, what: fn()) {
    let start = std::time::Instant::now();
    (what)();
    let elapsed = start.elapsed();
    info!("Task `{tag}` took {elapsed:#?}")
}
