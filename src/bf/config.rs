/// Configuration options.

use std::env;

use url::Url;

/// Defines the environment the library is operating with.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Environment {
    #[allow(dead_code)]
    Local,
    #[allow(dead_code)]
    Development,
    #[allow(dead_code)]
    Production
}

impl Environment {
    pub fn url(&self) -> Url {
        use self::Environment::*;
        match *self {
            Local => {
                let api_loc = env::var("BLACKFYNN_API_LOC").expect("BLACKFYNN_API_LOC must be defined");
                api_loc.parse::<Url>().expect(&format!("Not a valid url: {}", api_loc))
            },
            Development => "https://dev.blackfynn.io"
                .parse::<Url>()
                .unwrap(), // This should never fail
            Production => "https://app.blackfynn.io"
                .parse::<Url>()
                .unwrap() // This should never fail
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    env: Environment
}

impl Config {
    #[allow(dead_code)]
    pub fn new(env: Environment) -> Self {
        Config {
            env
        }
    }

    pub fn env(&self) -> &Environment {
        &self.env
    }
}
