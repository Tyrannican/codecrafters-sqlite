use anyhow::Result;
use clap::{Parser, Subcommand};
use sqlite::{cell::RecordValue, schema::SqliteSchema, SqliteReader};
use std::str::FromStr;

mod sqlite;

#[derive(Debug, Parser)]
struct Sqlite {
    /// Name of the Database to load
    dbname: String,

    /// Command to execute
    command: String,
}

fn main() -> Result<()> {
    let cli = Sqlite::parse();
    let db = SqliteReader::new(cli.dbname)?;

    match cli.command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.database_header.page_size);

            let page = db.page(0);
            println!("number of tables: {}", page.header.total_cells);
        }
        ".tables" => {
            use std::fmt::Write;
            let page = db.page(0);
            let cells = page.cells;
            let mut output = String::new();
            for cell in cells.iter() {
                let bt = cell.btree_leaf();
                match &bt.payload[2] {
                    RecordValue::String(table) => {
                        if !table.contains("sqlite") {
                            write!(output, "{table}")?;
                        }
                    }
                    _ => {}
                }
                write!(output, " ")?;
            }
            println!("{output}");
        }
        query => {
            let page = db.page(0);
            let schema = SqliteSchema::new(page);
            let query: Vec<&str> = query.split(" ").collect();
            let query_table = query.last().expect("not enough args");
            let table = schema.fetch_table(&query_table).unwrap();
            let table_page = db.page(table.root_page as usize);
            println!("{}", table_page.cells.len());
        }
    }

    Ok(())
}
