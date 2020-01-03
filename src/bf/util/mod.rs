// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Blackfynn library top-level utility code goes here.

pub mod futures;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

/// Generates an alphanumeric string of the given length.
#[allow(dead_code)]
pub fn rand_alphanum(length: usize) -> String {
    let rng = thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(length)
        .collect::<String>()
}

/// Adds a 6 character alphanumeric suffix to the input string.
#[allow(dead_code)]
pub fn rand_suffix<S>(input: S) -> String
where
    S: Into<String>,
{
    format!(
        "{input}-{suffix}",
        input = input.into(),
        suffix = rand_alphanum(6)
    )
}
