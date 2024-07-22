use anyhow::anyhow;
use anyhow::Result;

pub fn byte_size_str_to_usize(byte_size: &str) -> Result<usize> {
    let mut byte_str = byte_size.to_string();
    let k: usize = 1024;
    let m = k * 1024;
    let g = m * 1024;
    return match byte_str.chars().last() {
        Some(latest) => match latest {
            'k' | 'K' => {
                byte_str.remove(byte_str.len() - 1);
                let size: usize = byte_str.parse()?;
                let size = size * k;
                Ok(size)
            }
            'm' | 'M' => {
                byte_str.remove(byte_str.len() - 1);
                let size: usize = byte_str.parse()?;
                let size = size * m;
                Ok(size)
            }
            'g' | 'G' => {
                byte_str.remove(byte_str.len() - 1);
                let size: usize = byte_str.parse()?;
                let size = size * g;
                Ok(size)
            }
            _ => {
                let size: usize = byte_str.parse()?;
                Ok(size)
            }
        },
        None => return Err(anyhow!("unit description error")),
    };
}

pub fn byte_size_usize_to_str(byte_size: usize) -> String {
    return match byte_size {
        b if b < 1024 => b.to_string(),
        b if b >= 1024 && b < 1024 * 1024 => {
            let size = b / 1024;
            let mut size_str = size.to_string();
            size_str.push('k');
            size_str
        }

        b if b >= 1024 * 1024 && b < 1024 * 1024 * 1024 => {
            let size = b / 1024 / 1024;
            let mut size_str = size.to_string();
            size_str.push('m');
            size_str
        }

        _ => {
            let size = byte_size / 1024 / 1024 / 1024;
            let mut size_str = size.to_string();
            size_str.push('g');
            size_str
        }
    };
}
#[cfg(test)]
mod test {
    use crate::commons::{byte_size_str_to_usize, byte_size_usize_to_str};

    //cargo test commons::convert::test::test_byte_size_to_usize -- --nocapture
    #[test]
    fn test_byte_size_to_usize() {
        let r = byte_size_str_to_usize("12g");
        println!("{:?}", r);
    }

    //cargo test commons::convert::test::test_byte_size_usize_to_str -- --nocapture
    #[test]
    fn test_byte_size_usize_to_str() {
        let r = byte_size_usize_to_str(1073741823);
        println!("{:?}", r);
    }
}
