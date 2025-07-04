use anyhow::Result;
use memmap2::Mmap;
use schema::SqliteSchema;
use std::{fmt::Write, fs::File, path::Path};

use bytes::{Buf, Bytes};

pub mod cell;
pub mod page;
pub mod schema;
pub mod sql;

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

pub struct SqliteReader {
    reader: Mmap,
    pub database_header: DatabaseHeader,
}

// TODO: This will be the way forward
impl SqliteReader {
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

    pub fn page(&self, page: usize) -> BTreePage {
        let page_size = usize::from(self.database_header.page_size);
        let (start_offset, end_offset) = if page == 0 {
            (HEADER_SIZE, page_size)
        } else {
            (page * page_size, (page + 1) * page_size)
        };

        assert!(start_offset < self.reader.len());

        // TODO: Off by one somehow
        assert!(end_offset < self.reader.len() + 1);

        BTreePage::new(&self.reader[start_offset..end_offset], page)
    }

    pub fn schema(&self) -> SqliteSchema {
        let schema_page = self.page(0);
        SqliteSchema::new(schema_page)
    }

    pub fn dbinfo(&self) {
        println!("database page size: {}", self.database_header.page_size);

        let page = self.page(0);
        println!("number of tables: {}", page.header.total_cells);
    }

    pub fn tables(&self) -> Result<()> {
        let schema = self.schema();
        let tables = schema.tables();
        let mut output = String::new();
        for table in tables.into_iter() {
            if table.contains("sqlite") {
                continue;
            }

            write!(output, "{table} ")?;
        }
        println!("{}", output.trim());

        Ok(())
    }

    // Only supporting select statements for now
    pub fn query(&self, query: &str) -> Result<()> {
        let schema = self.schema();
        let (_, statement) = sql::select_statement(&query).unwrap();
        let Some(table) = schema.fetch_table(&statement.table) else {
            eprintln!("error: no such table '{}'", statement.table);
            return Ok(());
        };

        let table_page = self.page(table.root_page as usize);
        if statement.operation.is_some() {
            println!("{}", table_page.cells.len());
            return Ok(());
        }

        let table_schema = table.columns();
        let mut output_cols = Vec::new();
        for row in table_page.cells.iter() {
            let row = row.btree_leaf();
            let values = row.payload();
            let mut output = String::new();
            let mut col_iter = statement.columns.iter().peekable();
            while let Some(column_name) = col_iter.next() {
                let Some(idx) = table_schema
                    .columns
                    .iter()
                    .position(|c| &c.name == column_name)
                else {
                    eprintln!("error: no such column '{column_name}'");
                    return Ok(());
                };

                if let Some(cond) = &statement.where_clause {
                    if &cond.column == column_name && values[idx].to_string() != cond.value {
                        output.clear();
                        continue;
                    }
                }
                write!(output, "{}", values[idx])?;

                if col_iter.peek().is_some() {
                    write!(output, "|")?;
                }
            }

            if !output.is_empty() {
                output_cols.push(output);
            }
        }

        for o in output_cols {
            println!("{o}");
        }

        Ok(())
    }
}

pub fn parse_varint(buf: &[u8]) -> (i64, usize) {
    let mut varint: i64 = 0;
    let mut consumed = 0;

    // Varints are 9 bytes max
    for (i, byte) in buf.iter().enumerate().take(9) {
        consumed += 1;
        if i == 8 {
            varint = (varint << 8) | *byte as i64;
            break;
        }

        varint = (varint << 7) | (*byte & 0x7f) as i64;
        if *byte & 0x80 == 0 {
            break;
        }
    }

    (varint, consumed)
}
