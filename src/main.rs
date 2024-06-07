use crate::logger::init_log;
use logger::tracing_init;
mod checkers;
pub mod checkpoint;
mod cmd;
mod commons;
mod configure;

mod exception;
mod interact;
mod logger;
mod s3;
mod tasks;

fn main() {
    // init_log();
    tracing_init();
    cmd::run_app();
}
