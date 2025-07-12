use super::{
    parse_varint,
    sql::{ColumnDefinition, Condition},
};
use bytes::Buf;
use std::fmt::Write;

#[derive(Debug, Clone)]
pub enum DatabaseCell {
    LeafCell(LeafCell),
    IndexLeafCell(IndexLeafCell),
    InteriorTableCell(InteriorTableCell),
    InteriorIndexCell(InteriorIndexCell),
}

#[derive(Debug, Clone)]
pub(crate) struct LeafCell {
    pub row_id: i64,
    serial_types: Vec<RecordSerialType>,
    pub payload: Vec<RecordValue>,
    overflow_page: Option<u32>,
}

impl LeafCell {
    pub fn new(mut buf: &[u8]) -> Self {
        let (payload_size, consumed) = parse_varint(buf);
        buf.advance(consumed);

        let (row_id, consumed) = parse_varint(buf);
        buf.advance(consumed);

        let mut payload = &buf[..payload_size as usize];
        let (payload_header_size, consumed) = parse_varint(payload);
        payload.advance(consumed);

        let mut serial_types = vec![];
        let mut remaining_header_bytes = payload_header_size as usize - consumed;
        while remaining_header_bytes > 0 {
            let (value, consumed) = parse_varint(payload);
            payload.advance(consumed);
            remaining_header_bytes -= consumed;
            serial_types.push(RecordSerialType::from(value));
        }

        let payload = &buf[payload_header_size as usize..payload_size as usize];
        let payload_values = serial_types_to_record_values(&serial_types, payload);

        Self {
            row_id,
            serial_types,
            payload: payload_values,
            overflow_page: None, // Not used in this challenge
        }
    }

    pub fn query_row(
        &self,
        search_cols: &[String],
        schema_cols: &[ColumnDefinition],
        condition: &Option<Condition>,
    ) -> Result<String, String> {
        let mut output = String::new();
        let mut iter = search_cols.iter().peekable();
        if let Some(ref cond) = condition {
            let Some(idx) = schema_cols.iter().position(|c| &c.name == &cond.column) else {
                return Err(format!("error: no such column '{}'", cond.column));
            };

            let value = &self.payload[idx];
            if value.to_string() != cond.value {
                return Ok(String::new());
            }
        }

        while let Some(s_col) = iter.next() {
            let Some(idx) = schema_cols.iter().position(|c| &c.name == s_col) else {
                return Err(format!("error: no such column '{s_col}'"));
            };
            let value = &self.payload[idx];

            // Temporary
            if *value == RecordValue::Null && s_col == "id" {
                write!(output, "{}", self.row_id).unwrap();
            } else {
                write!(output, "{value}").unwrap();
            }
            if iter.peek().is_some() {
                write!(output, "|").unwrap();
            }
        }

        Ok(output)
    }
}

#[derive(Debug, Clone)]
pub struct InteriorTableCell {
    pub row_id: i64,
    pub left_child: u32,
}

impl InteriorTableCell {
    pub fn new(mut buf: &[u8]) -> Self {
        let left_child = buf.get_u32();
        let (row_id, consumed) = parse_varint(buf);
        buf.advance(consumed);

        Self {
            left_child: left_child - 1,
            row_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InteriorIndexCell {
    pub left_child: u32,
    pub key: String,
}

impl InteriorIndexCell {
    pub fn new(mut buf: &[u8]) -> Self {
        let left_child = buf.get_u32();
        let (_, consumed) = parse_varint(buf);
        buf.advance(consumed);

        let (header_size, consumed) = parse_varint(buf);
        buf.advance(consumed);

        let mut serial_types = Vec::new();
        let mut remaining_header_bytes = header_size as usize - consumed;
        while remaining_header_bytes > 0 {
            let (value, consumed) = parse_varint(buf);
            buf.advance(consumed);
            remaining_header_bytes -= consumed;
            serial_types.push(RecordSerialType::from(value));
        }

        let payload_values = serial_types_to_record_values(&serial_types, buf);

        let RecordValue::String(key) = &payload_values[0] else {
            panic!("only supporting string index keys");
        };

        Self {
            left_child: left_child - 1,
            key: key.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    pub key: String,
    pub row_id: i64,
}

impl IndexLeafCell {
    pub fn new(mut buf: &[u8]) -> Self {
        let (_, consumed) = parse_varint(buf);
        buf.advance(consumed);

        let (header_size, consumed) = parse_varint(buf);
        buf.advance(consumed);

        let mut serial_types = Vec::new();
        let mut remaining_header_bytes = header_size as usize - consumed;
        while remaining_header_bytes > 0 {
            let (value, consumed) = parse_varint(buf);
            buf.advance(consumed);
            remaining_header_bytes -= consumed;
            serial_types.push(RecordSerialType::from(value));
        }

        let payload_values = serial_types_to_record_values(&serial_types, buf);

        let RecordValue::String(key) = &payload_values[0] else {
            panic!("only supporting string index keys");
        };

        let row_id = match &payload_values[1] {
            RecordValue::I8(value) => *value as i64,
            RecordValue::I16(value) => *value as i64,
            RecordValue::I24(value) => *value as i64,
            RecordValue::I32(value) => *value as i64,
            RecordValue::I48(value) => *value as i64,
            RecordValue::I64(value) => *value,
            _ => panic!("only supporting numeric ids"),
        };

        Self {
            key: key.to_string(),
            row_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecordValue {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Blob(Vec<u8>),
    String(String),
}

impl std::fmt::Display for RecordValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::I8(i8) => write!(f, "{i8}"),
            Self::I16(i16) => write!(f, "{i16}"),
            Self::I24(i24) => write!(f, "{i24}"),
            Self::I32(i32) => write!(f, "{i32}"),
            Self::I48(i48) => write!(f, "{i48}"),
            Self::I64(i64) => write!(f, "{i64}"),
            Self::F64(f64) => write!(f, "{f64}"),
            Self::Bool(bool) => write!(f, "{bool}"),
            Self::Blob(blob) => write!(f, "blob ({} bytes)", blob.len()),
            Self::String(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum RecordSerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    False,
    True,
    Blob(usize),
    String(usize),
    Internal,
}

impl From<i64> for RecordSerialType {
    fn from(value: i64) -> Self {
        match value {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::F64,
            8 => Self::False,
            9 => Self::True,
            10 | 11 => Self::Internal,
            value if value >= 12 && value % 2 == 0 => Self::Blob(((value - 12) / 2) as usize),
            value if value >= 13 && value % 2 != 0 => Self::String(((value - 13) / 2) as usize),
            _ => panic!("invalid value for record serial type: {value}"),
        }
    }
}

fn serial_types_to_record_values(
    serial_types: &[RecordSerialType],
    mut buf: &[u8],
) -> Vec<RecordValue> {
    serial_types
        .iter()
        .map(|st| match *st {
            RecordSerialType::Null => RecordValue::Null,
            RecordSerialType::I8 => RecordValue::I8(buf.get_i8()),
            RecordSerialType::I16 => RecordValue::I16(buf.get_i16()),
            RecordSerialType::I24 => {
                let buf: [u8; 3] = [buf.get_u8(), buf.get_u8(), buf.get_u8()];
                let sign = if buf[0] & 0x80 != 0 { 0xFF } else { 0 };
                let bytes = [sign, buf[0], buf[1], buf[2]];
                RecordValue::I24(i32::from_be_bytes(bytes))
            }
            RecordSerialType::I32 => RecordValue::I32(buf.get_i32()),
            RecordSerialType::I48 => {
                let buf: [u8; 6] = [
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                ];
                let sign = if buf[0] & 0x80 != 0 { 0xFF } else { 0 };
                let bytes = [sign, sign, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]];
                RecordValue::I48(i64::from_be_bytes(bytes))
            }
            RecordSerialType::I64 => RecordValue::I64(buf.get_i64()),
            RecordSerialType::F64 => RecordValue::F64(buf.get_f64()),
            RecordSerialType::False => RecordValue::Bool(false),
            RecordSerialType::True => RecordValue::Bool(true),
            RecordSerialType::Blob(size) => {
                let blob = (0..size).into_iter().map(|_| buf.get_u8()).collect();
                RecordValue::Blob(blob)
            }
            RecordSerialType::String(size) => {
                let bytes: Vec<u8> = (0..size).into_iter().map(|_| buf.get_u8()).collect();
                RecordValue::String(String::from_utf8(bytes).expect("not utf8"))
            }
            _ => todo!("deal with internal"),
        })
        .collect::<Vec<RecordValue>>()
}
