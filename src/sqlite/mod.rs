use anyhow::Result;
use cell::{BTreeLeafCell, DatabaseCell};
use memmap2::Mmap;
use schema::SqliteSchema;
use sql::{CreateTable, SelectStatement};
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
            println!("{}", table_page.count());
            return Ok(());
        }

        let table_schema = table.columns();
        // This deals with a single cell
        // In the case of Interior pages, we need to deal with multiple cells
        // Some kind of feedback / recursion

        let mut tmp = Vec::new();
        for row in table_page.cells.iter() {
            match row {
                DatabaseCell::BTreeLeafCell(leaf) => {
                    let result = self.parse_cell(&statement, &table_schema, leaf);
                    tmp.push(result);
                }
                DatabaseCell::BTreeInteriorTableCell(interior) => {
                    let interior_page = self.page(interior.left_child as usize);
                    println!("INTERIOR PAGE");
                    // TODO: Deal with Index Leaf Cells
                    let results: Vec<Option<String>> = interior_page
                        .cells
                        .iter()
                        .map(|ir| {
                            self.parse_cell(&statement, &table_schema, ir.is_btree_leaf().unwrap())
                        })
                        .collect();
                    tmp.extend(results);
                }
            }
        }
        dbg!(tmp);

        let cols: Vec<String> = table_page
            .cells
            .iter()
            .filter_map(|row| {
                let Some(row) = row.is_btree_leaf() else {
                    let interior = row.is_btree_interior_table_cell().unwrap();
                    let page = self.page(interior.left_child as usize);
                    todo!();
                };
                match row.query_row(
                    &statement.columns,
                    &table_schema.columns,
                    &statement.where_clause,
                ) {
                    Ok(s) => {
                        if !s.is_empty() {
                            Some(s)
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        eprintln!("{e}");
                        None
                    }
                }
            })
            .collect();

        for result in cols {
            println!("{result}");
        }

        Ok(())
    }

    fn parse_cell(
        &self,
        statement: &SelectStatement,
        table_schema: &CreateTable,
        row: &BTreeLeafCell,
    ) -> Option<String> {
        match row.query_row(
            &statement.columns,
            &table_schema.columns,
            &statement.where_clause,
        ) {
            Ok(s) => {
                if !s.is_empty() {
                    Some(s)
                } else {
                    None
                }
            }
            Err(e) => {
                eprintln!("{e}");
                None
            }
        }
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
