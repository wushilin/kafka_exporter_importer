use super::errors;
use super::errors::GeneralError;
use super::util;
use http::{HeaderValue, Request};
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper::header::HeaderName;
use hyper::Client;
use hyper_rustls::{self, HttpsConnector};
use log::{debug, info, warn};
use rustls::{Certificate, ClientConfig};
use rustls_native_certs;
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct BasicAuthCredential {
    username: String,
    password: String,
}

impl BasicAuthCredential {
    pub fn from_strings(username: String, password: String) -> BasicAuthCredential {
        return BasicAuthCredential { username, password };
    }

    pub fn username(&self) -> String {
        return self.username.clone();
    }

    pub fn password(&self) -> String {
        return self.password.clone();
    }
}

#[derive(Debug, Clone)]
pub struct SrConfig {
    urls: Vec<String>,
    credential: Option<BasicAuthCredential>,
    truststore_location: Option<String>,
    trust_system_certs: bool,
}

impl SrConfig {
    pub fn from_url(url: String) -> SrConfig {
        return SrConfig {
            urls: vec![url],
            credential: None,
            truststore_location: None,
            trust_system_certs: true,
        };
    }

    pub fn trust_system_certs(&mut self, trust: bool) {
        self.trust_system_certs = trust;
    }

    pub fn is_system_cert_trusted(&self) -> bool {
        return self.trust_system_certs;
    }

    pub fn from_map(map: HashMap<String, String>) -> Result<SrConfig, Box<dyn Error>> {
        let sr_url = map.get("schema.registry.url");
        match sr_url {
            None => {
                return Err(errors::GeneralError::wrap_box(
                    "no `schema.registry.url` defined",
                ));
            }
            Some(url) => {
                let tokens = url.split(",");
                let mut urls_vec = Vec::new();
                for j in tokens {
                    let next_token = String::from(j);
                    let trimmed = String::from(next_token.trim());
                    urls_vec.push(trimmed);
                }
                let mut result = SrConfig::from_url(urls_vec.first().unwrap().clone());
                for i in 1..urls_vec.len() {
                    result.add_url(urls_vec.get(i).unwrap().clone());
                }
                let source = map.get("basic.auth.credentials.source");
                let credential = map.get("basic.auth.user.info");
                let ca_location = map.get("schema.registry.ssl.truststore.location");
                let trust_system_o =
                    map.get("schema.registry.ssl.truststore.include.system.default");
                let mut trust_system = true;
                match trust_system_o {
                    None => {}
                    Some(trust_str) => {
                        trust_system = String::from("true").eq_ignore_ascii_case(trust_str);
                    }
                }
                result.trust_system_certs(trust_system);
                if source.is_some() && credential.is_some() {
                    let source = source.unwrap();
                    if source != "USER_INFO" {
                        return Err(errors::GeneralError::wrap_box(
                            "`basic.auth.credentials.source` must be set to `USER_INFO`",
                        ));
                    }
                    let credential = credential.unwrap();
                    let tokens: Vec<&str> = credential.split(":").collect();
                    if tokens.len() != 2 {
                        return Err(GeneralError::wrap_box(
                            "`basic.auth.user.info` must be in `username:password` format",
                        ));
                    }
                    result.set_credential(BasicAuthCredential {
                        username: String::from(tokens[0]),
                        password: String::from(tokens[1]),
                    });
                }
                match ca_location {
                    Some(location) => {
                        result.set_truststore_location(location.clone());
                    }
                    None => {}
                }
                return Ok(result);
            }
        }
    }

    pub fn add_url(&mut self, new_url: String) {
        self.urls.push(new_url)
    }

    pub fn urls(&self) -> Vec<String> {
        let mut result = Vec::with_capacity(self.urls.len());
        for next in &self.urls {
            result.push(next.clone());
        }
        return result;
    }

    pub fn set_credential(&mut self, cred: BasicAuthCredential) {
        self.credential = Some(cred);
    }

    pub fn set_truststore_location(&mut self, path: String) {
        self.truststore_location = Some(path);
    }

    pub fn truststore_location(&self) -> Option<String> {
        match self.truststore_location.as_ref() {
            None => None,
            Some(location) => Some(location.clone()),
        }
    }

    pub fn credential(&self) -> Option<BasicAuthCredential> {
        match self.credential.as_ref() {
            None => None,
            Some(credential) => Some(BasicAuthCredential::from_strings(
                credential.username(),
                credential.password(),
            )),
        }
    }
}

pub struct SrClient {
    client: hyper::Client<HttpsConnector<HttpConnector>, Body>,
    sr_config: SrConfig,
    cache: HashMap<i32, String>,
}

impl SrClient {
    pub fn from_map(map: HashMap<String, String>) -> Result<SrClient, Box<dyn Error>> {
        let config = SrConfig::from_map(map)?;
        return Self::from_config(config);
    }
    pub fn from_config(sr_config: SrConfig) -> Result<SrClient, Box<dyn Error>> {
        return Ok(SrClient {
            client: Client::builder().build(SrClient::build_connector(&sr_config)?),
            sr_config,
            cache: HashMap::new(),
        });
    }
    fn build_connector(config: &SrConfig) -> Result<HttpsConnector<HttpConnector>, Box<dyn Error>> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(get_tls_config(
                &config.truststore_location,
                config.trust_system_certs,
            )?)
            .https_or_http()
            .enable_http1()
            .build();
        return Ok(https);
    }

    pub async fn get_schema_by_id(&mut self, id: i32) -> Result<String, Box<dyn Error>> {
        let cached_result = self.cache.get(&id);
        if let Some(result) = cached_result {
            debug!("Cache hit {id}");
            return Ok(result.clone());
        }
        debug!("Cache miss");
        let result = self.get_schema_by_id_inner(id).await;
        match result {
            Err(cause) => {
                return Err(cause);
            }
            Ok(result) => {
                let result_c = result.clone();
                self.cache.insert(id, result);
                return Ok(result_c);
            }
        }
    }

    pub async fn execute_each(
        &mut self,
        method: hyper::Method,
        uri_suffix: String,
        in_headers: Option<HashMap<String, String>>,
        body: Option<String>,
    ) -> Result<String, Box<dyn Error>> {
        let mut urls = self.sr_config.urls();

        for (index, next) in urls.iter().enumerate() {
            let actual_uri = format!("{next}{uri_suffix}");
            let uri = actual_uri.parse()?;
            let next_result = self
                .execute(method.clone(), uri, in_headers.clone(), body.clone())
                .await;
            match next_result {
                Err(cause) => {
                    if index < urls.len() - 1 {
                        warn!("Schema registry `{next}` didn't work: {cause:#?}. Trying next")
                    } else {
                        warn!("Schema registry `{next}` didn't work: {cause:#?}. No more left")
                    }
                }
                Ok(result) => {
                    if index != 0 {
                        info!("Schema registry `{next}` worked. Swapping with 0");
                        urls.swap(0, index);
                        self.sr_config.urls.clear();
                        self.sr_config.urls.extend(urls);
                    }
                    return Ok(result);
                }
            }
        }
        return Err(GeneralError::wrap_box("None of `schema.registry.url` worked"));
    }
    async fn execute(
        &mut self,
        method: hyper::Method,
        uri: http::uri::Uri,
        in_headers: Option<HashMap<String, String>>,
        body: Option<String>,
    ) -> Result<String, Box<dyn Error>> {
        let mut req = Request::default();
        let headers = req.headers_mut();
        if let Some(credential) = &self.sr_config.credential {
            let header_value = gen_basic(&credential.username, &credential.password);
            headers.insert("Authorization", HeaderValue::from_str(&header_value)?);
        }
        if let Some(x) = in_headers {
            for (k, v) in x {
                headers.insert(
                    HeaderName::from_str(k.as_str())?,
                    HeaderValue::from_str(&v)?,
                );
            }
        }
        *req.method_mut() = method;
        *req.uri_mut() = uri;
        if body.is_some() {
            let body_mut = req.body_mut();
            let body_real = body.unwrap();
            *body_mut = Body::from(body_real);
        }
        let client = &self.client;
        let res: Result<http::Response<Body>, hyper::Error> = client.request(req).await;
        match res {
            Err(cause) => {
                return Err(Box::new(cause));
            }
            Ok(body) => {
                let status = body.status();
                let body_result = tokio::task::spawn_blocking(move || {
                    return hyper::body::to_bytes(body);
                })
                .await?
                .await?;
                let body_str_u8 = body_result.to_vec();
                let utf = String::from_utf8(body_str_u8)?;
                if status.as_u16() < 200 || status.as_u16() >= 400 {
                    return Err(GeneralError::wrap_box(
                        format!("Unexpected response code {status}. Body {utf}").as_str(),
                    ));
                }
                return Ok(utf);
            }
        }
    }

    pub async fn register_schema(
        &mut self,
        subject: &str,
        schema: &str,
    ) -> Result<i32, Box<dyn Error>> {
        // POST /subjects/(string: subject)/versions
        let uri_suffix = format!("/subjects/{subject}/versions");
        let method = hyper::Method::POST;
        let mut in_headers = HashMap::new();
        in_headers.insert(String::from("Accept"), String::from("application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"));
        in_headers.insert(
            String::from("Content-Type"),
            String::from("application/vnd.schemaregistry.v1+json"),
        );

        let body = Some(String::from(schema));
        let headers = Some(in_headers);
        let result = self.execute_each(method, uri_suffix, headers, body).await;
        match result {
            Err(cause) => {
                return Err(cause);
            }
            Ok(result_str) => {
                let out_value: serde_json::Value = serde_json::from_str(&result_str)?;
                let id_value = out_value.get("id");
                match id_value {
                    None => {
                        return Err(GeneralError::wrap_box("no `id` field in response JSON!"));
                    }
                    Some(id_value) => match id_value {
                        serde_json::Value::Number(number) => {
                            return Ok(number.as_i64().unwrap() as i32);
                        }
                        _ => {
                            return Err(GeneralError::wrap_box(
                                "`id` is not a number in response JSON!",
                            ));
                        }
                    },
                }
            }
        }
    }

    async fn get_schema_by_id_inner(&mut self, id: i32) -> Result<String, Box<dyn Error>> {
        let actual_suffix = format!("/schemas/ids/{id}");
        let result = self
            .execute_each(hyper::Method::GET, actual_suffix, None, None)
            .await;
        return result;
    }
}

fn gen_basic(username: &str, password: &str) -> String {
    let tmp = format!("{username}:{password}");
    let tmp = util::base64_encode(tmp.as_bytes());
    let tmp = format!("Basic {tmp}");
    return tmp;
}

fn get_tls_config(
    ca_location: &Option<String>,
    trust_system_certs: bool,
) -> Result<ClientConfig, Box<dyn Error>> {
    let mut store = rustls::RootCertStore::empty();
    if trust_system_certs {
        let native_certs = rustls_native_certs::load_native_certs()?;
        for next in native_certs {
            store.add(&rustls::Certificate(next.0))?;
        }
    }
    if !ca_location.is_none() {
        let location = ca_location.as_ref().unwrap();
        let file = File::open(location)?;
        let mut reader = BufReader::new(file);
        let certs_data = rustls_pemfile::certs(&mut reader)?;
        let certs: Vec<Certificate> = certs_data.into_iter().map(Certificate).collect();
        for cert in certs {
            store.add(&cert)?;
        }
    }

    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(store)
        .with_no_client_auth();

    Ok(config)
}
