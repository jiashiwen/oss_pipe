use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::{
    cmp::min,
    fmt::Write,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

/// 进度条，使用时在主线程之外的线程使用
pub fn exec_processbar(
    total: u64,
    stop_mark: Arc<AtomicBool>,
    status_map: Arc<DashMap<String, usize>>,
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
        thread::sleep(Duration::from_millis(200));
        let offset: Option<usize> = status_map
            .iter()
            .filter(|f| f.key().starts_with(key_prefix))
            .map(|m| *m.value())
            .min();
        match offset {
            Some(size) => {
                let current: u64 = size.try_into().unwrap();
                let new = min(current, total);
                pb.set_position(new);
            }
            None => {}
        }
    }
    pb.finish_with_message("Finish");
}
