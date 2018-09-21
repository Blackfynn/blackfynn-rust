// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Library configuration options and environment definitions.

use std::env;

use url::Url;

use bf::model::S3ServerSideEncryption;

/// Defines the server environment the library is interacting with.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
pub enum Environment {
    #[allow(dead_code)]
    Local,
    #[allow(dead_code)]
    Development,
    #[allow(dead_code)]
    Production,
}

impl Environment {
    pub fn url(&self) -> Url {
        use self::Environment::*;
        match *self {
            Local => {
                let api_loc =
                    env::var("BLACKFYNN_API_LOC").expect("BLACKFYNN_API_LOC must be defined");
                api_loc
                    .parse::<Url>()
                    .unwrap_or_else(|_| panic!("Not a valid url: {}", api_loc))
            }
            Development => "https://dev.blackfynn.io".parse::<Url>().unwrap(), // This should never fail
            Production => "https://api.blackfynn.io".parse::<Url>().unwrap(), // This should never fail
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
    pub fn api_url(&self) -> Url {
        self.env.url()
    }

    #[allow(dead_code)]
    pub fn s3_server_side_encryption(&self) -> &S3ServerSideEncryption {
        &self.s3_server_side_encryption
    }
}
