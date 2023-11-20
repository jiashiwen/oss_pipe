pub fn size_distributed(size: i128) -> String {
    return match size {
        s if s < 1024 * 1024 => "1M".to_string(),
        s if s >= 1024 * 1024 && s < 1024 * 1024 * 10 => "0-10M".to_string(),
        s if s >= 1024 * 1024 * 10 && s < 1024 * 1024 * 100 => "10-100M".to_string(),
        s if s >= 1024 * 1024 * 100 && s < 1024 * 1024 * 300 => "100-300M".to_string(),
        s if s >= 1024 * 1024 * 300 && s < 1024 * 1024 * 500 => "300-500M".to_string(),
        s if s >= 1024 * 1024 * 500 && s < 1024 * 1024 * 1024 => "500-1024M".to_string(),
        _ => "1G+".to_string(),
    };
}
