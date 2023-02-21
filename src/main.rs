use crate::configure::set_config;
use crate::logger::init_log;

mod checkers;
mod cmd;
mod commons;
mod configure;
mod httpquerry;
mod logger;
mod s3;

#[tokio::main]
async fn main() {
    init_log();
    cmd::run_app();
}
