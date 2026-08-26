//! Product CLI: `hedron import` and `hedron hql`. No daemon, no listen port.

use std::env;
use std::process;

use hedron_core::{hql, import};

const ROOT_HELP: &str = "\
HedronDB product CLI.

Usage:
  hedron <COMMAND> [OPTIONS]

Commands:
  import  Load a markdown tree into a HedronDB store
  hql     Run a read-only HQL v0 pipeline

Options:
  -h, --help  Print help

`hedron-import` is a thin alias of `hedron import`.
Python `python/hql` is a result-twin of `hedron hql`.
";

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run(mut args: Vec<String>) -> Result<(), String> {
    if args.is_empty() {
        print!("{ROOT_HELP}");
        return Ok(());
    }
    let cmd = args.remove(0);
    match cmd.as_str() {
        "-h" | "--help" | "help" => {
            print!("{ROOT_HELP}");
            Ok(())
        }
        "import" => import::run_cli(args),
        "hql" => hql::run_cli(args),
        other => Err(format!(
            "unknown command {other:?}\n\nRun `hedron --help` for usage."
        )),
    }
}
