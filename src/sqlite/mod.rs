use anyhow::Result;
use cell::{DatabaseCell, InteriorTableCell, LeafCell, RecordValue};
use memmap2::Mmap;
use schema::{SchemaTable, SqliteSchema};
use sql::{CreateTable, SelectStatement};
use std::{fmt::Write, fs::File, path::Path};

use bytes::{Buf, Bytes};

pub mod cell;
pub mod page;
pub mod schema;
pub mod sql;

use page::{BTreePage, BTreePageType};

const HEADER_SIZE: usize = 100;

#[derive(Debug, Copy, Clone)]
pub struct DatabaseHeader {
    magic: [u8; 16],
    pub page_size: u16,
    write_version: u8,
    read_version: u8,
    reserved_space: u8,
    max_payload: u8,
    min_payload: u8,
    leaf_payload: u8,
    file_change_counter: u32,
    in_header_database_size: u32,
    freelist_trunk_page_page_no: u32,
    total_freelist_pages: u32,
    schema_cookie: u32,
    schema_format_number: u32,
    default_page_cache_size: u32,
    largest_root_b_tree_page: u32,
    text_encoding: u32,
    user_version: u32,
    incremental_vacuum_mode: u32,
    application_id: u32,
    reserved_expansion: [u8; 20],
    version_valid_for_number: u32,
    sqlite_version_number: u32,
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

        match statement.where_clause {
            Some(_) => match schema.fetch_index(&statement.table) {
                Some(idx) => self.index_scan(idx, table, &statement),
                None => self.full_table_scan(table, &statement),
            },
            None => self.full_table_scan(table, &statement),
        }
    }

    fn full_table_scan(&self, table: &SchemaTable, statement: &SelectStatement) -> Result<()> {
        let table_page = self.page(table.root_page as usize);
        if statement.operation.is_some() {
            println!("{}", table_page.count());
            return Ok(());
        }

        let table_schema = table.columns();
        let rows = self.traverse_rows(&table_page.cells);
        let cols: Vec<String> = rows
            .iter()
            .filter_map(|row| self.parse_row(&statement, &table_schema, row))
            .collect();

        for result in cols {
            println!("{result}");
        }

        Ok(())
    }

    fn index_scan(
        &self,
        index: &SchemaTable,
        table: &SchemaTable,
        statement: &SelectStatement,
    ) -> Result<()> {
        let index_page = self.page(index.root_page as usize);
        let search_key = &statement.where_clause.as_ref().unwrap().value;
        let row_ids = self.parse_cells_test(&index_page, &search_key);
        dbg!(row_ids);

        todo!("parsing index");
    }

    // FIX: There's a missing row in the tests
    // Am I even parsing them right...?
    fn parse_cells_test(&self, page: &BTreePage, search_key: &str) -> Vec<i64> {
        let mut row_ids = Vec::new();
        let cells = &page.cells;
        let rightmost = page.right_page_pointer();

        for cell in cells {
            match cell {
                DatabaseCell::IndexLeafCell(leaf) => {
                    if search_key == leaf.key {
                        row_ids.push(leaf.row_id);
                    }
                }
                DatabaseCell::InteriorIndexCell(interior_table) => {
                    if search_key <= interior_table.key.as_str() {
                        let page = self.page(interior_table.left_child as usize);
                        row_ids.extend(self.parse_cells_test(&page, search_key));
                    } else {
                        let right_page = rightmost.unwrap();
                        let page = self.page(right_page as usize);
                        row_ids.extend(self.parse_cells_test(&page, search_key));
                    }
                }
                other => todo!("{other:#?} rows"),
            }
        }

        row_ids
    }

    fn traverse_index_rows(&self, page: &BTreePage, id: i64) -> Option<LeafCell> {
        None
    }

    fn traverse_rows(&self, cells: &[DatabaseCell]) -> Vec<LeafCell> {
        let mut rows = vec![];

        for cell in cells.iter() {
            match cell {
                DatabaseCell::LeafCell(leaf) => rows.push(leaf.clone()),
                DatabaseCell::InteriorTableCell(interior_table) => {
                    let page = self.page(interior_table.left_child as usize);
                    let interior_cells = self.traverse_rows(&page.cells[..]);
                    rows.extend(interior_cells);
                }
                _ => todo!("traversing rows"),
            }
        }

        rows
    }

    fn parse_row(
        &self,
        statement: &SelectStatement,
        table_schema: &CreateTable,
        row: &LeafCell,
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
