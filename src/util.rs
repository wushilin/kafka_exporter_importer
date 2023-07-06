use base64::{engine::general_purpose, Engine as _};
use log::info;
use super::errors;

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

pub fn open_new(path:&str) -> Result<std::fs::File, errors::GeneralError> {
    let mut opts = std::fs::OpenOptions::new();
    let opts = opts.create_new(true).write(true);
    let result = opts.open(path);
    match result {
        Ok(result) => {
            return Ok(result);
        },
        Err(_) => {
            return Err(errors::GeneralError::from_string( 
                format!("Can't open {path} as new file. make sure it is writable & does not already exist")
            ));
        }
    }
}
pub fn replace_schema_id(data:&mut [u8], id:i32) {
    if data[0] != 0x00 {
        panic!("Invalid magic byte!");
    }
    let new_bytes = i32::to_be_bytes(id);
    for i in 0..new_bytes.len() {
        data[i+1] = new_bytes[i];
    }
}
pub fn get_schema_id(data: Option<&[u8]>) -> Option<i32> {
    let bytes = data?;
    if bytes.len() < 5 {
        return None;
    }
    let magic_byte = bytes[0];
    if magic_byte != 0 {
        return None;
    }
    let schema_id = &bytes[1..5];
    let schema_id_bytes: [u8; 4] = match schema_id {
        [a, b, c, d] => [*a, *b, *c, *d],
        _ => panic!("Slice length is not 4"),
    };
    let schema_id = i32::from_be_bytes(schema_id_bytes);
    return Some(schema_id);
}