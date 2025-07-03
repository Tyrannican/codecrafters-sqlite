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
        ".dbinfo" => {
            println!("database page size: {}", db.database_header.page_size);

            let page = db.page(0);
            println!("number of tables: {}", page.header.total_cells);
        }
        ".tables" => {
            use std::fmt::Write;
            let schema = db.schema();
            let tables = schema.tables();
            let mut output = String::new();
            for table in tables.into_iter() {
                if table.contains("sqlite") {
                    continue;
                }

                write!(output, "{table} ")?;
            }
            println!("{}", output.trim());
        }
        query => {
            let schema = db.schema();
            // Only supporting select statements for now
            let (_, statement) = sqlite::sql::select_statement(&query).unwrap();
            let table = schema.fetch_table(&statement.table);
            assert!(table.is_some());
            let table = table.unwrap();

            let sql = table.sql.to_owned();
            sqlite::sql::create_statement(Box::leak(sql.into_boxed_str()))?;
            let table_page = db.page(table.root_page as usize);

            // TODO: Refactor
            match statement.operation {
                // Count only
                Some(_) => {
                    println!("{}", table_page.count());
                }
                None => {
                    let columns = statement.columns;
                    // println!("{:#?}", table_page.cells);
                }
            }
        }
    }

    Ok(())
}
