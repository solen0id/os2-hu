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
    let mut addresses_per_block = 0;
    let mut indirect_address_block = 0;
    let mut double_indirect_adress_block = 0;
    let mut triple_indirect_adress_block = 0;

    for block in e2fs.block_iter {
        if addresses_per_block == 0 {
            addresses_per_block = block.len()/4;
            indirect_address_block = 12+1;
            double_indirect_adress_block = indirect_address_block + addresses_per_block +1;
            triple_indirect_adress_block = double_indirect_adress_block + (addresses_per_block +1)*addresses_per_block +1;

        }
        if image_detected {
            block_nr += 1;
            // 1-12 direct data-blocks
            // skip indirect address block
            if block_nr == indirect_address_block {println!("Skipped dir {}", block_nr); continue;}

            // skip double indirect address block
            if block_nr == double_indirect_adress_block {println!("Skipped dou {}", block_nr); continue;}
            // skip indirect address blocks
            if block_nr > double_indirect_adress_block && block_nr <triple_indirect_adress_block &&
                (block_nr-double_indirect_adress_block) % (1+addresses_per_block) == 1 {println!("Skipped doudir {}", block_nr); continue; }
            //if block_nr > 14+addresses_per_block/4 && (block_nr - (14+addresses_per_block/4)) % (1+addresses_per_block/4) == 1{ continue; }

            // skip triple indirect address block
            if block_nr == triple_indirect_adress_block {println!("Skipped tri {}", block_nr); continue;}
            // skip double indirect address-blocks
            if block_nr > triple_indirect_adress_block &&
                (block_nr-triple_indirect_adress_block) % ((1+addresses_per_block)*addresses_per_block +1) == 1 {println!("Skipped tridou {}", block_nr); continue; }
            // skip indirect address blocks
            if  block_nr > triple_indirect_adress_block &&
                (((block_nr - triple_indirect_adress_block - 1) % ((1+addresses_per_block)*addresses_per_block +1))-1)%(1+addresses_per_block) == 0 {println!("Skipped tridir {}", block_nr); continue;}
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
