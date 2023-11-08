use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::{
    cmp::min,
    fmt::Write,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::task::yield_now;

use crate::checkpoint::FilePosition;

/// 进度条，使用时在主线程之外的线程使用
pub async fn exec_processbar(
    total: u64,
    stop_mark: Arc<AtomicBool>,
    status_map: Arc<DashMap<String, FilePosition>>,
    key_prefix: &str,
) {
    let pb = ProgressBar::new(total);
    let progress_style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("#>-");
    pb.set_style(progress_style);

    while !stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let line_num: Option<usize> = status_map
            .iter()
            .filter(|f| f.key().starts_with(key_prefix))
            .map(|m| m.line_num)
            .min();
        match line_num {
            Some(size) => {
                let current: u64 = size.try_into().unwrap();
                let new = min(current, total);
                pb.set_position(new);
            }
            None => {}
        }
        yield_now().await;
    }
    pb.set_position(total);
    pb.finish_with_message("Finish");
}
