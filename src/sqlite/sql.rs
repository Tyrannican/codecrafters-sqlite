use nom::{
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace0},
    combinator::{map, opt},
    multi::separated_list1,
    sequence::{delimited, preceded},
    IResult, Parser,
};

#[derive(Debug)]
pub struct SelectStatement {
    pub operation: Option<Operation>,
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<Condition>,
}

#[derive(Debug)]
pub struct Condition {
    pub column: String,
    pub value: String,
}

#[derive(Debug)]
pub enum Operation {
    Count, // For now, only COUNT(*) is supported
}

fn identifier(input: &str) -> IResult<&str, String> {
    let (input, ident) = take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '*')(input)?;
    Ok((input, ident.to_string()))
}

fn operation(input: &str) -> IResult<&str, Option<Operation>> {
    opt(map(
        (
            multispace0,
            tag_no_case("count"),
            tag("("),
            char('*'),
            tag(")"),
            multispace0,
        ),
        |_| Operation::Count,
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
        take_while1(|c: char| c.is_alphanumeric() || c == '\'' || c == '_'),
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

fn where_clause(input: &str) -> IResult<&str, Option<Condition>> {
    opt(preceded(
        (multispace0, tag_no_case("where"), multispace0),
        condition,
    ))
    .parse(input)
}

pub fn select_statement(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = (tag_no_case("select"), multispace0).parse(input)?;
    let (input, operation) = operation(input)?;

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
