use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use bytes::Buf;

#[derive(Debug)]
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

// TODO: Fix
impl From<[u8; 100]> for DatabaseHeader {
    fn from(value: [u8; 100]) -> Self {
        Self {
            magic: value[..16].try_into().expect("error taking magic string"),
            page_size: u16::from_be_bytes(value[16..18].try_into().unwrap()),
            write_version: u8::from_be(value[18]),
            read_version: u8::from_be(value[19]),
            reserved_space: u8::from_be(value[20]),
            max_payload: u8::from_be(value[21]),
            min_payload: u8::from_be(value[22]),
            leaf_payload: u8::from_be(value[23]),
            file_change_counter: u32::from_be_bytes(value[24..28].try_into().unwrap()),
            in_header_database_size: u32::from_be_bytes(value[28..32].try_into().unwrap()),
            freelist_trunk_page_page_no: u32::from_be_bytes(value[32..36].try_into().unwrap()),
            total_freelist_pages: u32::from_be_bytes(value[36..40].try_into().unwrap()),
            schema_cookie: u32::from_be_bytes(value[40..44].try_into().unwrap()),
            schema_format_number: u32::from_be_bytes(value[44..48].try_into().unwrap()),
            default_page_cache_size: u32::from_be_bytes(value[48..52].try_into().unwrap()),
            largest_root_b_tree_page: u32::from_be_bytes(value[52..56].try_into().unwrap()),
            text_encoding: u32::from_be_bytes(value[56..60].try_into().unwrap()),
            user_version: u32::from_be_bytes(value[60..64].try_into().unwrap()),
            incremental_vacuum_mode: u32::from_be_bytes(value[64..68].try_into().unwrap()),
            application_id: u32::from_be_bytes(value[68..72].try_into().unwrap()),
            reserved_expansion: value[72..92].try_into().unwrap(),
            version_valid_for_number: u32::from_be_bytes(value[92..96].try_into().unwrap()),
            sqlite_version_number: u32::from_be_bytes(value[96..100].try_into().unwrap()),
        }
    }
}
