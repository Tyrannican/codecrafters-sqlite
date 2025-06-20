use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use sqlite::DatabaseParser;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::str::FromStr;

mod sqlite;

#[derive(Debug, Parser)]
struct Sqlite {
    /// Name of the Database to load
    dbname: String,

    /// Command to execute
    #[arg()]
    command: SqliteCommand,
}

#[derive(Subcommand, Debug, Clone)]
enum SqliteCommand {
    /// Display information about the database
    DbInfo,
}

impl FromStr for SqliteCommand {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            ".dbinfo" => Ok(SqliteCommand::DbInfo),
            _ => Err(format!("unknown command: {}", s)),
        }
    }
}

fn main() -> Result<()> {
    // Parse arguments
    let cli = Sqlite::parse();
    match cli.command {
        SqliteCommand::DbInfo => {
            let mut db = DatabaseParser::new(cli.dbname)?;
            let header = db.header()?;
            // let db = File::open(cli.dbname)?;
            // let mut reader = BufReader::new(db);
            // let mut header_bytes = [0; 100];
            // reader.read_exact(&mut header_bytes)?;

            // let header = sqlite::DatabaseHeader::from(header_bytes);
            // let page_size = header.page_size;

            // println!("database page size: {page_size}");

            // let mut schema_page = [0; 4096 - 100];
            // reader.read_exact(&mut schema_page)?;
            // let total_tables = u16::from_be_bytes(schema_page[3..5].try_into()?);
            // println!("number of tables: {total_tables}");
        }
    }

    Ok(())
}
