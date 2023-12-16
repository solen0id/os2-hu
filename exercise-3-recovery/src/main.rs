use std::io::Write;
use std::{
    fs::{self, File},
    io,
};
mod ext2;

const JPEG_STRT_B1: u8 = 0b11111111;
const JPEG_STRT_B2: u8 = 0b11011000;
const JPEG_END_B1: u8 = 0b11111111;
const JPEG_END_B2: u8 = 0b11011001;
const DEFAULT: u8 = 0;

fn recover_files(_device: fs::File, _path: &str) -> io::Result<()> {
    let e2fs = ext2::Ext2FS::new(_device);
    e2fs.print_debug();

    let mut images: Vec<Vec<u8>> = Vec::new();
    let mut image: Vec<u8> = Vec::new();
    let mut image_detected: bool = false;
    let mut block_nr = 0;

    for block in e2fs.block_iter {
        // skip non-data blocks
        if block_nr == 13 || block_nr == 270 || block_nr == 65806{
            block_nr += 1;
            continue;
        }
        for byte in block {
            if image_detected {
                if byte == JPEG_END_B2 && *image.last().unwrap_or(&DEFAULT) == JPEG_END_B1 {
                    println!("found image end");
                    image.push(byte);
                    images.push(image.clone());
                    image.clear();
                    image_detected = false;
                    block_nr = 0;
                } else {
                    image.push(byte);
                }
            } else {
                // image start not detected yet
                if byte == JPEG_STRT_B1 && image.is_empty() {
                    image.push(byte);
                } else if byte == JPEG_STRT_B1 {
                    continue;
                } else if byte == JPEG_STRT_B2 && *image.last().unwrap_or(&DEFAULT) == JPEG_STRT_B1
                {
                    println!("found image start");
                    image.push(byte);
                    image_detected = true;
                    block_nr = 1;
                } else if !image.is_empty() {
                    image.clear();
                }
            }
        }
        if image_detected {
            block_nr += 1;
        }
    }

    let mut i = 0;

    for image in images {
        let image_path = format!("{}/image_{}.jpg", _path, i);
        let mut file = File::create(image_path)?;
        file.write_all(&image)?;
        i += 1;
    }

    return Ok(());
}

fn main() -> io::Result<()> {
    use std::env::args;
    let device_path = args().nth(1).unwrap_or("examples/small.img".to_string());
    let target_path = args().nth(2).unwrap_or("restored/small/".to_string());

    fs::create_dir_all(&target_path)?;
    recover_files(fs::File::open(&device_path)?, &target_path)
}
