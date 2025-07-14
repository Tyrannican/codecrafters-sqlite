use anyhow::Result;
use cell::{DatabaseCell, IndexLeafCell, InteriorTableCell, LeafCell, RecordValue};
use memmap2::Mmap;
use schema::{SchemaTable, SqliteSchema};
use sql::{CreateTable, SelectStatement};
use std::{fmt::Write, fs::File, hash::Hash, path::Path};

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
        let rows = self.traverse_rows(&table_page);
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
        let mut row_ids = Vec::new();
        let search_key = &statement.where_clause.as_ref().unwrap().value;
        self.search_index(&index_page, &search_key, &mut row_ids);

        let mut target_rows = Vec::new();
        let table_page = self.page(table.root_page as usize);
        for id in row_ids {
            self.traverse_indexed_rows(&table_page, id, &mut target_rows);
        }

        let table_schema = table.columns();
        let cols: Vec<String> = target_rows
            .iter()
            .filter_map(|row| self.parse_row(&statement, &table_schema, row))
            .collect();

        for result in cols {
            println!("{result}");
        }
        Ok(())
    }

    fn search_index(&self, page: &BTreePage, search_key: &str, row_ids: &mut Vec<u64>) {
        match page.page_type() {
            BTreePageType::InteriorIndex => {
                let mut recursed_left = false;
                for cell in page.cells.iter() {
                    let DatabaseCell::InteriorIndexCell(index_cell) = cell else {
                        panic!("expected an interior index cell - found {cell:#?}");
                    };

                    let index_key = index_cell.key.as_str();
                    if search_key < index_key {
                        let left_page = self.page(index_cell.left_child as usize);
                        self.search_index(&left_page, search_key, row_ids);
                        recursed_left = true;
                    } else if index_key == search_key {
                        row_ids.push(index_cell.row_id);
                        let left_page = self.page(index_cell.left_child as usize);
                        self.search_index(&left_page, search_key, row_ids);
                        recursed_left = true;
                    }
                }

                if !recursed_left {
                    if let Some(rp) = page.right_page_pointer() {
                        let right_page = self.page(rp as usize);
                        self.search_index(&right_page, search_key, row_ids);
                    }
                }
            }
            BTreePageType::LeafIndex => {
                for cell in page.cells.iter() {
                    let DatabaseCell::IndexLeafCell(leaf) = cell else {
                        panic!("expected index leaf cell - found {cell:#?}");
                    };

                    if leaf.key == search_key {
                        row_ids.push(leaf.row_id);
                    }
                }
            }
            _ => {}
        }
    }

    fn traverse_indexed_rows(&self, page: &BTreePage, id: u64, target_rows: &mut Vec<LeafCell>) {
        let mut recursed_left = false;
        for cell in page.cells.iter() {
            match cell {
                DatabaseCell::InteriorTableCell(table_cell) => {
                    if id <= table_cell.row_id {
                        let left_page = self.page(table_cell.left_child as usize);
                        self.traverse_indexed_rows(&left_page, id, target_rows);
                        recursed_left = true;
                    }
                }
                DatabaseCell::LeafCell(leaf) => {
                    if id == leaf.row_id {
                        target_rows.push(leaf.clone());
                    }
                }
                _ => panic!(),
            }
        }

        if !recursed_left {
            if let Some(rp) = page.right_page_pointer() {
                let right_page = self.page(rp as usize);
                self.traverse_indexed_rows(&right_page, id, target_rows);
            }
        }
    }

    // FIX: Rework this to be cleaner
    fn traverse_rows(&self, page: &BTreePage) -> Vec<LeafCell> {
        let mut rows = vec![];
        let cells = &page.cells;

        for cell in cells.iter() {
            match cell {
                DatabaseCell::LeafCell(leaf) => rows.push(leaf.clone()),
                DatabaseCell::InteriorTableCell(interior_table) => {
                    let page = self.page(interior_table.left_child as usize);
                    let interior_cells = self.traverse_rows(&page);
                    rows.extend(interior_cells);

                    if let Some(rpp) = page.right_page_pointer() {
                        let right_page = self.page(rpp as usize);
                        let interior_cells = self.traverse_rows(&right_page);
                        rows.extend(interior_cells);
                    }
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

    fn index_dump(&self) {
        let index_cells: Vec<IndexLeafCell> = (0..1910)
            .into_iter()
            .flat_map(|page_no| {
                let page = self.page(page_no);
                let index_leaves: Vec<IndexLeafCell> = page
                    .cells
                    .into_iter()
                    .filter_map(|cell| {
                        if let DatabaseCell::IndexLeafCell(leaf) = cell {
                            return Some(leaf.clone());
                        } else {
                            None
                        }
                    })
                    .collect();

                index_leaves
            })
            .collect();

        let mut row_ids: Vec<u64> = index_cells.into_iter().map(|idx| idx.row_id).collect();
        row_ids.sort();
        let mut s = String::new();
        for id in row_ids.iter() {
            write!(s, "{id}\n").unwrap();
        }
        print!("{s}");
    }
}

pub fn parse_varint(buf: &[u8]) -> (u64, usize) {
    let mut varint: u64 = 0;
    let mut consumed = 0;

    // Varints are 9 bytes max
    for (i, byte) in buf.iter().enumerate().take(9) {
        consumed += 1;
        if i == 8 {
            varint = (varint << 8) | *byte as u64;
            break;
        }

        varint = (varint << 7) | (*byte & 0x7f) as u64;
        if *byte & 0x80 == 0 {
            break;
        }
    }

    (varint, consumed)
}
