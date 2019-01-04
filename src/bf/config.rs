// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Library configuration options and environment definitions.

use std::env;
use std::fmt;
use std::str::FromStr;

use url::Url;

use bf::error::{Error, ErrorKind};
use bf::model::S3ServerSideEncryption;

/// Defines the server environment the library is interacting with.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Environment {
    #[allow(dead_code)]
    Local,
    #[allow(dead_code)]
    Development,
    #[allow(dead_code)]
    Production,
}

/// Service definition, containing the URL of the service.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Service {
    #[allow(dead_code)]
    API,
    #[allow(dead_code)]
    Analytics,
    #[allow(dead_code)]
    Concepts,
}

impl Environment {
    pub fn service_url(self, service: Service) -> Url {
        use self::Environment::*;
        match (self, service) {
            (Local, Service::API) => {
                let api_loc =
                    env::var("BLACKFYNN_API_LOC").expect("BLACKFYNN_API_LOC must be defined");
                api_loc
                    .parse::<Url>()
                    .unwrap_or_else(|_| panic!("Not a valid url: {}", api_loc))
            }
            (Local, s) => panic!("Local environment not supported for {:?}", s),
            (Development, Service::API) => "https://dev.blackfynn.io".parse::<Url>().unwrap(), // This should never fail
            (Production, Service::API) => "https://api.blackfynn.io".parse::<Url>().unwrap(),
            (Development, Service::Analytics) => "https://dev-graph-view-service-use1.blackfynn.io"
                .parse::<Url>()
                .unwrap(),
            (Production, Service::Analytics) => "https://prod-graph-view-service-use1.blackfynn.io"
                .parse::<Url>()
                .unwrap(),
            (Development, Service::Concepts) => "https://concepts.dev.blackfynn.io:443"
                .parse::<Url>()
                .unwrap(),
            (Production, Service::Concepts) => {
                "https://concepts.blackfynn.io".parse::<Url>().unwrap()
            }
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let printable = match *self {
            Environment::Local => "local",
            Environment::Development => "development",
            Environment::Production => "production",
        };

        write!(f, "{}", printable)
    }
}

impl FromStr for Environment {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_ref() {
            "dev" | "development" => Ok(Environment::Development),
            "prod" | "production" => Ok(Environment::Production),
            "local" => Ok(Environment::Local),
            _ => Err(ErrorKind::EnvParseError(s.to_string()).into()),
        }
    }
}

/// Configuration options for the Blackfynn client.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    env: Environment,
    s3_server_side_encryption: S3ServerSideEncryption,
}

impl Config {
    #[allow(dead_code)]
    pub fn new(env: Environment) -> Self {
        Self {
            s3_server_side_encryption: Default::default(),
            env,
        }
    }

    #[allow(dead_code)]
    pub fn env(&self) -> &Environment {
        &self.env
    }

    #[allow(dead_code)]
    pub fn api_service(&self) -> Url {
        self.env.service_url(Service::API)
    }

    #[allow(dead_code)]
    pub fn analytics_service(&self) -> Url {
        self.env.service_url(Service::Analytics)
    }

    #[allow(dead_code)]
    pub fn concepts_service(&self) -> Url {
        self.env.service_url(Service::Concepts)
    }

    #[allow(dead_code)]
    pub fn s3_server_side_encryption(&self) -> &S3ServerSideEncryption {
        &self.s3_server_side_encryption
    }
}
