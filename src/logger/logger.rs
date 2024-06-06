use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use tracing_appender::rolling;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Layer};

pub fn init_log() {
    let window_size = 3; // log0, log1, log2
    let fixed_window_roller = FixedWindowRoller::builder()
        .build("log/app-{}.log", window_size)
        .unwrap();
    let size_limit = 100 * 1024 * 1024; // 100M as max log file size to roll
    let size_trigger = SizeTrigger::new(size_limit);
    let compound_policy =
        CompoundPolicy::new(Box::new(size_trigger), Box::new(fixed_window_roller));
    let rolling_file = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build("logs/app.log", Box::new(compound_policy))
        .unwrap();

    let file_out = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} {l} {t} - {m}{n}")))
        .build("logs/app.log")
        .unwrap();
    // let stdout = ConsoleAppender::builder()
    //     .encoder(Box::new(JsonEncoder::new()))
    //     .build();
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{l} {d} {t} {f} {L} - {m}{n}",
        )))
        .build();

    // let appender_rolling = Appender::builder().build("rolling_file", Box::new(rolling_file));
    // let appender_file = Appender::builder().build("file_out", Box::new(file_out));
    // let appender_out = Appender::builder().build("stdout", Box::new(stdout));

    let config = Config::builder()
        .appender(Appender::builder().build("rolling_file", Box::new(rolling_file)))
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file_out", Box::new(file_out)))
        // .logger(Logger::builder().build("app::backend::db", LevelFilter::Info))
        // .logger(Logger::builder()
        //     .appender("requests")
        //     .additive(false)
        //     .build("app::requests", LevelFilter::Info))
        // .build(Root::builder().appender("rolling_file").build(LevelFilter::Debug))
        .build(
            Root::builder()
                .appender("stdout")
                .appender("file_out")
                .build(LevelFilter::Info),
        )
        .unwrap();

    let _ = log4rs::init_config(config).unwrap();
}

pub fn tracing_init() {
    // 格式化输出层，并且输出到终端。
    let formatting_layer = fmt::layer()
        .pretty()
        .with_file(true)
        .with_line_number(true)
        .with_writer(std::io::stdout)
        .with_filter(tracing_subscriber::filter::LevelFilter::INFO)
        .boxed();

    // 文件输出层
    let file_appender = rolling::daily("logs/", "oss_pip.log");
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_file(true)
        .with_line_number(true)
        .with_writer(file_appender)
        .with_filter(tracing_subscriber::filter::LevelFilter::INFO) // 文件输出日志等级
        .boxed();

    let registry = tracing_subscriber::registry()
        .with(file_layer)
        .with(formatting_layer);

    registry.init()
}
