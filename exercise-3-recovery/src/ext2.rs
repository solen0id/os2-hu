// implementation template for convenience
// see https://www.nongnu.org/ext2-doc/ext2.html for documentation on ext2 fs
use std::convert::TryInto;
use std::{fs, os::unix::fs::FileExt};

#[derive(Debug)]
pub struct Ext2FS {
    pub superblock: Superblock,
    pub block_iter: BlockIter,
    block_group_descriptor: BlockGroupDescriptor,
}

#[derive(Debug)]
pub struct BlockIter {
    bitmap: Vec<u8>,
    data: Vec<Vec<u8>>,
    bitmap_ix: usize,
    done: bool,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct Superblock {
    // block info
    s_blocks_count: u32,
    s_block_size: u32,
    s_blocks_per_group: u32,
    s_first_data_block: u32,
    // inode info
    s_inodes_count: u32,
    s_inodes_per_group: u32,

    // derived values
    bg_desc_block_id: u32,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
struct BlockGroupDescriptor {
    bg_block_bitmap: u32, // 0
    bg_inode_bitmap: u32, // 4
    bg_inode_table: u32,  // 8
}

impl Superblock {
    pub fn new(bytes: [u8; 1024]) -> Self {
        let s_blocks_count = parse_field_u32(&bytes, 4);
        let s_block_size = 1024 << parse_field_u32(&bytes, 24);
        let s_blocks_per_group = parse_field_u32(&bytes, 32);
        let s_first_data_block = parse_field_u32(&bytes, 20);
        let s_inodes_count = parse_field_u32(&bytes, 0);
        let s_inodes_per_group = parse_field_u32(&bytes, 40);

        // the block descriptor table comes right after the superblock
        let bg_desc_block_id = s_first_data_block + 1;

        // All our examples contain just 1 blockgroup, so we can keep it simple
        if s_blocks_count > s_blocks_per_group || s_inodes_count > s_inodes_per_group {
            unimplemented!()
        }

        let superblock = Superblock {
            s_blocks_count,
            s_block_size,
            s_blocks_per_group,
            s_first_data_block,
            s_inodes_count,
            s_inodes_per_group,
            bg_desc_block_id,
        };

        superblock
    }
}

impl BlockGroupDescriptor {
    pub fn new(bytes: &[u8]) -> Self {
        let bg_block_bitmap = parse_field_u32(bytes, 0);
        let bg_inode_bitmap = parse_field_u32(bytes, 4);
        let bg_inode_table = parse_field_u32(bytes, 8);

        BlockGroupDescriptor {
            bg_block_bitmap,
            bg_inode_bitmap,
            bg_inode_table,
        }
    }
}

impl Ext2FS {
    pub fn new(file: fs::File) -> Self {
        // parse Superblock
        let mut superblock_bytes = [0u8; 1024];
        let _bytes_read = file
            .read_at(&mut superblock_bytes, 1024)
            .expect("Could not read superblock bytes");
        let superblock = Superblock::new(superblock_bytes);
        let blocksize: usize = superblock.s_block_size.try_into().unwrap();

        // parse BlockGroupDescriptor
        let mut bg_desc_bytes = vec![0u8; blocksize];
        let bg_desc_byte_offset = superblock.bg_desc_block_id * superblock.s_block_size;
        let _bytes_read = file
            .read_at(&mut bg_desc_bytes, bg_desc_byte_offset.into())
            .expect("Could not read block descriptor bytes");
        let block_group_descriptor = BlockGroupDescriptor::new(&bg_desc_bytes);

        // parse block bitmap
        let mut bitmap_bytes = vec![0u8; blocksize];
        let bitmap_bytes_offset = block_group_descriptor.bg_block_bitmap * superblock.s_block_size;
        let _bytes_read = file
            .read_at(&mut bitmap_bytes, bitmap_bytes_offset.into())
            .expect("Could not read block bitmap bytes");

        // parse data blocks
        let data_bytes_size: usize = bitmap_bytes.len() * 8 * blocksize;

        // TODO: Investigate why setting data_bytes_offset=1024 produces a jpeg
        // WITHOUT artefacts for small.img, but not any of the other file systems..?
        //
        // let data_bytes_offset = superblock.s_first_data_block * superblock.s_block_size;
        let data_bytes_offset: u32 = 0;

        let mut data_bytes: Vec<u8> = vec![0u8; data_bytes_size];

        let _bytes_read = file
            .read_at(&mut data_bytes, data_bytes_offset.into())
            .expect("Could not read data block bytes");
        let nested_data_bytes: Vec<Vec<u8>> = data_bytes
            .chunks(blocksize)
            .map(|chunk| chunk.to_vec())
            .collect();

        // create BlockIterator
        let block_iter = BlockIter {
            bitmap: bitmap_bytes,
            data: nested_data_bytes,
            bitmap_ix: 0,
            done: false,
        };

        Ext2FS {
            superblock,
            block_group_descriptor,
            block_iter,
        }
    }

    pub fn print_debug(&self) {
        // let x = self.superblock.s_blocks_per_group;
        println!("{:?}", self.superblock);
        println!("{:?}", self.block_group_descriptor);
    }
}

impl BlockIter {
    fn get_bitmap_at(&self, index: usize) -> Option<u8> {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index > self.bitmap.len() {
            return None;
        }

        let bm_byte = self.bitmap.get(byte_index)?;
        let bm_bit = bm_byte >> bit_index & 1;

        return Some(bm_bit);
    }
}

impl Iterator for BlockIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.get_bitmap_at(self.bitmap_ix) {
                Some(0) => break,
                Some(1) => {
                    self.bitmap_ix += 1;
                    continue;
                }
                None => {
                    self.done = true;
                    break;
                }
                _ => continue,
            }
        }

        if self.done {
            println!("Done");
            return None;
        }

        let block = self.data.get(self.bitmap_ix).cloned();
        self.bitmap_ix += 1;

        return block;
    }
}

fn parse_field_u32(bytes: &[u8], offset: usize) -> u32 {
    let byte_array: [u8; 4] = bytes[offset..offset + 4].try_into().expect(&format!(
        "Could not parse Superblock u32 field at offset {} !",
        offset
    ));

    u32::from_le_bytes(byte_array)
}
