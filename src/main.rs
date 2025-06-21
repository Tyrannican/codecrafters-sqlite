use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use sqlite::SqliteReader;
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

    /// Display table information
    Tables,
}

impl FromStr for SqliteCommand {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            ".dbinfo" => Ok(SqliteCommand::DbInfo),
            ".tables" => Ok(SqliteCommand::Tables),
            _ => Err(format!("unknown command: {}", s)),
        }
    }
}

fn main() -> Result<()> {
    // Parse arguments
    let cli = Sqlite::parse();
    let mut db = SqliteReader::new(cli.dbname)?;

    match cli.command {
        SqliteCommand::DbInfo => {
            let header = db.header()?;
            println!("database page size: {}", header.page_size);

            let mut iter = db.into_iter();
            let page = iter.next().unwrap();
            println!("number of tables: {}", page.header.total_cells);
        }
        SqliteCommand::Tables => {
            let mut iter = db.into_iter();
            let page = iter.next().unwrap();
        }
    }

    Ok(())
}
