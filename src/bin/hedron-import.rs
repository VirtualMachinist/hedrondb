//! Thin alias of `hedron import`. Tokens stay in-process.

use std::env;
use std::process;

fn main() {
    if let Err(err) = hedron_core::import::run_cli(env::args().skip(1).collect()) {
        eprintln!("{err}");
        process::exit(1);
    }
}
