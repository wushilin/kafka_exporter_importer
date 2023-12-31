#[derive(Debug, Clone)]
pub struct GeneralError {
    msg: String,
}

impl std::error::Error for GeneralError {}

impl std::fmt::Display for GeneralError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl GeneralError {
    pub fn from_string(msg:String) -> GeneralError {
        return GeneralError{ msg };
    }
    
    pub fn wrap(msg: &str) -> GeneralError {
        return GeneralError { msg: String::from(msg) };
    }

    pub fn wrap_box(msg: &str) -> Box<GeneralError> {
        return Box::new(Self::wrap(msg));
    }
}
