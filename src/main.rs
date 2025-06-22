use anyhow::Result;
use clap::{Parser, Subcommand};
use sqlite::SqliteReader;
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
    let cli = Sqlite::parse();
    let db = SqliteReader::new(cli.dbname)?;

    match cli.command {
        SqliteCommand::DbInfo => {
            println!("database page size: {}", db.database_header.page_size);

            let page = db.page(0);
            println!("number of tables: {}", page.header.total_cells);
        }
        SqliteCommand::Tables => {
            //
        }
    }

    Ok(())
}
