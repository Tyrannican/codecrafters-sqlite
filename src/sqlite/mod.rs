use anyhow::{Context, Result};
use memmap2::Mmap;
use std::{
    fs::File,
    io::{BufRead, BufReader, Cursor, Read, Seek},
    path::Path,
};

use bytes::{Buf, Bytes};

pub mod page;
use page::BTreePage;

const HEADER_SIZE: usize = 100;

#[derive(Debug, Copy, Clone)]
pub struct DatabaseHeader {
    pub magic: [u8; 16],
    pub page_size: u16,
    pub write_version: u8,
    pub read_version: u8,
    pub reserved_space: u8,
    pub max_payload: u8,
    pub min_payload: u8,
    pub leaf_payload: u8,
    pub file_change_counter: u32,
    pub in_header_database_size: u32,
    pub freelist_trunk_page_page_no: u32,
    pub total_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    pub largest_root_b_tree_page: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    pub reserved_expansion: [u8; 20],
    pub version_valid_for_number: u32,
    pub sqlite_version_number: u32,
}

impl DatabaseHeader {
    pub fn new(buf: &[u8]) -> Self {
        let mut buf = Bytes::copy_from_slice(buf);
        let mut magic = [0; 16];
        let mut reserved_expansion = [0; 20];

        Self {
            magic: {
                buf.copy_to_slice(&mut magic);
                magic
            },
            page_size: buf.get_u16(),
            write_version: buf.get_u8(),
            read_version: buf.get_u8(),
            reserved_space: buf.get_u8(),
            max_payload: buf.get_u8(),
            min_payload: buf.get_u8(),
            leaf_payload: buf.get_u8(),
            file_change_counter: buf.get_u32(),
            in_header_database_size: buf.get_u32(),
            freelist_trunk_page_page_no: buf.get_u32(),
            total_freelist_pages: buf.get_u32(),
            schema_cookie: buf.get_u32(),
            schema_format_number: buf.get_u32(),
            default_page_cache_size: buf.get_u32(),
            largest_root_b_tree_page: buf.get_u32(),
            text_encoding: buf.get_u32(),
            user_version: buf.get_u32(),
            incremental_vacuum_mode: buf.get_u32(),
            application_id: buf.get_u32(),
            reserved_expansion: {
                buf.copy_to_slice(&mut reserved_expansion);
                reserved_expansion
            },
            version_valid_for_number: buf.get_u32(),
            sqlite_version_number: buf.get_u32(),
        }
    }
}

pub struct SqliteReaderMemMap {
    reader: Mmap,
    database_header: DatabaseHeader,
}

// TODO: This will be the way forward
impl SqliteReaderMemMap {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = File::open(path)?;
        // Safety: As this reader will only be instantiated in read contexts
        // we can guarantee that no one else will be modifying the underlying
        // file
        let reader = unsafe { Mmap::map(&db)? };
        let database_header = DatabaseHeader::new(&reader[0..HEADER_SIZE]);

        Ok(Self {
            reader,
            database_header,
        })
    }

    pub fn header(&self) -> DatabaseHeader {
        let header_bytes = &self.reader[0..HEADER_SIZE];
        DatabaseHeader::new(&header_bytes)
    }
}

pub struct SqliteReader {
    // TODO: Move to MemMapped file
    reader: BufReader<File>,
    header: Option<DatabaseHeader>,
}

impl SqliteReader {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = File::open(path)?;
        let reader = BufReader::new(db);

        Ok(Self {
            reader,
            header: None,
        })
    }

    pub fn header(&mut self) -> Result<&DatabaseHeader> {
        if let Some(ref header) = self.header {
            return Ok(header);
        }

        let mut header_bytes = [0; HEADER_SIZE];
        self.reader.read_exact(&mut header_bytes)?;
        let header = DatabaseHeader::new(&header_bytes);
        self.header = Some(header);

        Ok(self.header.as_ref().unwrap())
    }
}

impl Iterator for SqliteReader {
    type Item = BTreePage;
    fn next(&mut self) -> Option<Self::Item> {
        let page_size = match self.header {
            Some(header) => header.page_size,
            None => {
                let header = match self.header() {
                    Ok(h) => h,
                    Err(_) => panic!("error parsing header"),
                };

                header.page_size
            }
        };

        let current_position = self.reader.stream_position().ok()?;
        let page_buffer_len = if current_position % u64::from(page_size) != 0 {
            page_size - HEADER_SIZE as u16
        } else {
            page_size
        };

        // TODO: Deal with EOF
        let mut page_buffer = vec![0u8; usize::from(page_buffer_len)];
        self.reader.read_exact(&mut page_buffer).ok()?;

        Some(BTreePage::new(&page_buffer))
    }
}
