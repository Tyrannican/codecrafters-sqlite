use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufRead, BufReader, Cursor, Read},
    path::Path,
};

use bytes::{Buf, Bytes};

pub mod page;
use page::Page;

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

pub struct DatabaseParser {
    reader: BufReader<File>,
    header: Option<DatabaseHeader>,
}

impl DatabaseParser {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = File::open(path)?;
        let reader = BufReader::new(db);

        Ok(Self {
            reader,
            header: None,
        })
    }

    pub fn header(&mut self) -> Option<&DatabaseHeader> {
        if let Some(ref header) = self.header {
            return Some(header);
        }

        // TODO: Parse header
        let mut header_bytes = [0; 100];
        self.reader.read_exact(&mut header_bytes).ok()?;
        None
    }
}

impl Iterator for DatabaseParser {
    type Item = Page;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
