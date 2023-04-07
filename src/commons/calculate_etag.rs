use anyhow::Result;
use crypto::digest::Digest;
use crypto::md5::Md5;
use std::fs::File;
use std::io::prelude::*;
use std::iter::repeat;

fn calculate_etag_from_read(f: &mut dyn Read, chunk_size: usize) -> Result<String> {
    let mut md5 = Md5::new();
    let mut concat_md5 = Md5::new();
    let mut input_buffer = vec![0u8; chunk_size];
    let mut chunk_count = 0;
    let mut current_md5: Vec<u8> = repeat(0).take((md5.output_bits() + 7) / 8).collect();

    let md5_result = loop {
        let amount_read = f.read(&mut input_buffer)?;
        if amount_read > 0 {
            md5.reset();
            md5.input(&input_buffer[0..amount_read]);
            chunk_count += 1;
            md5.result(&mut current_md5);
            concat_md5.input(&current_md5);
        } else {
            if chunk_count > 1 {
                break format!("{}-{}", concat_md5.result_str(), chunk_count);
            } else {
                break md5.result_str();
            }
        }
    };
    Ok(md5_result)
}

fn calculate_etag(file: &str, chunk_size: usize) -> Result<String> {
    let mut f = File::open(file)?;
    calculate_etag_from_read(&mut f, chunk_size)
}

#[cfg(test)]
mod test {

    use super::calculate_etag;

    //cargo test commons::calculate_etag::test::test_calculate_etag -- --nocapture
    #[test]
    fn test_calculate_etag() {
        let r = calculate_etag("/tmp/gen/tmp", 8);
        println!("test scan result {:?}", r);
    }
}
