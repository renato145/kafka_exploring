use anyhow::Result;
use log::{set_max_level, LevelFilter};
use simple_logger::SimpleLogger;

pub fn set_logger(verbose: i32) -> Result<()> {
    SimpleLogger::new().init()?;
    let log_level = match verbose {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    set_max_level(log_level);
    Ok(())
}
