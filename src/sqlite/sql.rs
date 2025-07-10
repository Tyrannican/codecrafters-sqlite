use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt},
    multi::separated_list1,
    sequence::{delimited, preceded},
    IResult, Parser,
};

#[derive(Debug)]
pub struct SelectStatement {
    pub operation: Option<SelectOperation>,
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<Condition>,
}

#[derive(Debug)]
pub enum CreateStatement {
    Table(CreateTable),
    Index(CreateIndex),
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug)]
pub struct CreateIndex {
    name: String,
    table: String,
    table_column: String,
}

#[derive(Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub datatype: String,
    pub constraints: Vec<String>,
}

#[derive(Debug)]
pub struct Condition {
    pub column: String,
    pub value: String,
}

#[derive(Debug)]
pub enum SelectOperation {
    Count, // For now, only COUNT(*) is supported
}

fn identifier(input: &str) -> IResult<&str, String> {
    let (input, ident) =
        take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '*' || c == '\"')(input)?;
    Ok((input, ident.to_string()))
}

fn select_operation(input: &str) -> IResult<&str, Option<SelectOperation>> {
    opt(map(
        (
            multispace0,
            tag_no_case("count"),
            tag("("),
            char('*'),
            tag(")"),
            multispace0,
        ),
        |_| SelectOperation::Count,
    ))
    .parse(input)
}

fn column_list(input: &str) -> IResult<&str, Vec<String>> {
    separated_list1(delimited(multispace0, char(','), multispace0), identifier).parse(input)
}

fn condition(input: &str) -> IResult<&str, Condition> {
    let (input, (column, _, value)) = (
        identifier,
        delimited(multispace0, char('='), multispace0),
        take_while1(|c: char| c.is_alphanumeric() || c == '\'' || c == '_' || c == ' '),
    )
        .parse(input)?;

    Ok((
        input,
        Condition {
            column,
            value: value.trim_matches('\'').to_string(),
        },
    ))
}

fn constraint(input: &str) -> IResult<&str, String> {
    let keywords = alt((
        tag_no_case("primary key"),
        tag_no_case("autoincrement"),
        tag_no_case("not null"),
    ));
    map(preceded(multispace1, keywords), |s: &str| s.to_lowercase()).parse(input)
}

fn multiple_constraints(mut input: &str) -> IResult<&str, Vec<String>> {
    let mut constraints = Vec::new();
    while let Ok((next, cons)) = constraint(input) {
        constraints.push(cons);
        input = next;
    }

    Ok((input, constraints))
}

fn column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, _) = opt(multispace0).parse(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, datatype) = identifier(input)?;
    let (input, constraints) = multiple_constraints(input)?;

    Ok((
        input,
        ColumnDefinition {
            name,
            datatype,
            constraints,
        },
    ))
}

fn where_clause(input: &str) -> IResult<&str, Option<Condition>> {
    opt(preceded(
        (multispace0, tag_no_case("where"), multispace0),
        condition,
    ))
    .parse(input)
}

pub fn select_statement(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = (tag_no_case("select"), multispace0).parse(input)?;
    let (input, operation) = select_operation(input)?;

    // TODO: Fix this to be a bit cleaner
    if operation.is_some() {
        let (input, _) = (multispace0, tag_no_case("from"), multispace0).parse(input)?;
        let (input, table) = identifier(input)?;
        return Ok((
            input,
            SelectStatement {
                operation,
                columns: Vec::new(),
                table,
                where_clause: None,
            },
        ));
    }

    let (input, columns) = column_list(input)?;
    let (input, _) = (multispace0, tag_no_case("from"), multispace0).parse(input)?;
    let (input, table) = identifier(input)?;
    let (input, where_clause) = where_clause(input)?;
    let (input, _) = opt(char(';')).parse(input)?;

    Ok((
        input,
        SelectStatement {
            operation: None,
            columns,
            table,
            where_clause,
        },
    ))
}

pub fn create_statement(input: &str) -> IResult<&str, CreateStatement> {
    let (input, (_, _, table_name, _)) = (
        tag_no_case("create table"),
        multispace0,
        identifier,
        multispace0,
    )
        .parse(input)?;

    let (input, column_definition) = delimited(
        char('('),
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            column_definition,
        ),
        preceded(multispace0, char(')')),
    )
    .parse(input)?;

    Ok((
        input,
        CreateStatement::Table(CreateTable {
            name: table_name,
            columns: column_definition,
        }),
    ))
}
