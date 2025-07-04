use anyhow::Result;
use clap::Parser;
use sqlite::SqliteReader;

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
        ".dbinfo" => db.dbinfo(),
        ".tables" => db.tables()?,
        query => {
            db.query(&query)?;
            // match statement.operation {
            //     // Count only
            //     Some(_) => {
            //         println!("{}", table_page.count());
            //     }
            //     None => {
            //         let columns = statement.columns;
            //         for column in columns.iter() {
            //             if let Some(idx) =
            //                 table_columns.columns.iter().position(|c| &c.name == column)
            //             {
            //                 println!("IDX: {idx}");
            //                 println!("Table: {:#?}", table_page.cells[0]);
            //             }
            //         }
            //         println!("Columns: {columns:#?}");
            //     }
            // }
        }
    }

    Ok(())
}
