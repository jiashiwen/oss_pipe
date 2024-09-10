pub fn size_distributed(size: i128) -> String {
    return match size {
        s if s < 1024 => "0-1K".to_string(),
        s if s >= 1024 && s < 1024 * 8 => "1K-8K".to_string(),
        s if s >= 1024 * 8 && s < 1024 * 16 => "8K-16K".to_string(),
        s if s >= 1024 * 16 && s < 1024 * 32 => "16K-32K".to_string(),
        s if s >= 1024 * 32 && s < 1024 * 64 => "32K-64K".to_string(),
        s if s >= 1024 * 64 && s < 1024 * 128 => "64K-128K".to_string(),
        s if s >= 1024 * 128 && s < 1024 * 256 => "128K-512K".to_string(),
        s if s >= 1024 * 256 && s < 1024 * 512 => "256K-512K".to_string(),
        s if s >= 1024 * 512 && s < 1024 * 1024 => "512K-1M".to_string(),
        s if s >= 1024 * 1024 && s < 1024 * 1024 * 8 => "1M-8M".to_string(),
        s if s >= 1024 * 1024 * 8 && s < 1024 * 1024 * 16 => "8M-16M".to_string(),
        s if s >= 1024 * 1024 * 16 && s < 1024 * 1024 * 32 => "16M-32M".to_string(),
        s if s >= 1024 * 1024 * 32 && s < 1024 * 1024 * 64 => "32M-64M".to_string(),
        s if s >= 1024 * 1024 * 64 && s < 1024 * 1024 * 128 => "64M-128M".to_string(),
        s if s >= 1024 * 1024 * 128 && s < 1024 * 1024 * 256 => "128M-256M".to_string(),
        s if s >= 1024 * 1024 * 256 && s < 1024 * 1024 * 512 => "256M-512M".to_string(),
        s if s >= 1024 * 1024 * 512 && s < 1024 * 1024 * 1024 => "512M-1G".to_string(),
        _ => "1G+".to_string(),
    };
}
