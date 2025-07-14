use super::cell::{DatabaseCell, RecordValue};
use super::page::{BTreePage, BTreePageType};
use super::sql::{self, CreateIndex, CreateStatement, CreateTable};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct SqliteSchema {
    tables: BTreeMap<String, SchemaTable>,
}

impl SqliteSchema {
    pub fn new(page: BTreePage) -> Self {
        assert!(page.header.page_type == BTreePageType::LeafTable);

        let mut tables = BTreeMap::default();
        for cell in page.cells.iter() {
            let table = SchemaTable::new(cell);
            tables.insert(table.name.clone(), table);
        }

        Self { tables }
    }

    pub fn fetch_index(&self, table: &str) -> Option<&SchemaTable> {
        for value in self.tables.values() {
            if value.table_name == table && &value.sqlite_type == "index" {
                return Some(value);
            }
        }

        None
    }

    pub fn fetch_table(&self, table: &str) -> Option<&SchemaTable> {
        self.tables.get(table)
    }

    pub fn tables(&self) -> Vec<&str> {
        self.tables.keys().map(|t| t.as_str()).collect()
    }
}

#[derive(Debug)]
pub struct SchemaTable {
    sqlite_type: String,
    pub name: String,
    pub table_name: String,
    pub root_page: u64,
    pub sql: String,
}

impl SchemaTable {
    pub fn new(cell: &DatabaseCell) -> Self {
        match cell {
            DatabaseCell::LeafCell(inner) => {
                assert!(inner.payload.len() == 5);
                let RecordValue::String(sqlite_type) = &inner.payload[0] else {
                    panic!("expected a string(sqlite_type)");
                };

                let RecordValue::String(name) = &inner.payload[1] else {
                    panic!("expected a string(name)");
                };

                let RecordValue::String(table_name) = &inner.payload[2] else {
                    panic!("expected a string(table_name)");
                };

                let root_page = match &inner.payload[3] {
                    RecordValue::I8(value) => *value as u64,
                    RecordValue::I16(value) => *value as u64,
                    RecordValue::I24(value) => *value as u64,
                    RecordValue::I32(value) => *value as u64,
                    RecordValue::I48(value) => *value as u64,
                    RecordValue::I64(value) => *value as u64,
                    other => panic!("expected an integer(root_page) - found {other:#?}"),
                };

                let RecordValue::String(sql) = &inner.payload[4] else {
                    panic!("exptected a string(sql)");
                };

                return Self {
                    sqlite_type: sqlite_type.clone(),
                    name: name.clone(),
                    table_name: table_name.clone(),
                    root_page: root_page - 1,
                    sql: sql.clone(),
                };
            }
            _ => todo!(),
        }
    }

    pub fn indexes(&self) -> CreateIndex {
        let (_, create_statement) =
            sql::create_statement(&self.sql).expect("should parse create statement");
        match create_statement {
            CreateStatement::Index(i) => i,
            _ => panic!("expected index, found something else"),
        }
    }

    pub fn columns(&self) -> CreateTable {
        let (_, create_statement) =
            sql::create_statement(&self.sql).expect("should parse create statement");
        match create_statement {
            CreateStatement::Table(t) => t,
            _ => panic!("expected table, found something else"),
        }
    }
}
