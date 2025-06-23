use super::parse_varint;
use bytes::Buf;

#[derive(Debug)]
pub enum DatabaseCell {
    BTreeLeafCell(BTreeLeafCell),
}

#[derive(Debug)]
pub(crate) struct BTreeLeafCell {
    record_header_size: u64,
    row_id: u64,
    serial_types: Vec<RecordSerialType>,
    payload: Vec<RecordValue>,
    overflow_page: Option<u32>,
}

impl BTreeLeafCell {
    pub fn new(mut buf: &[u8]) -> Self {
        let (payload_size, consumed) = parse_varint(buf);
        dbg!(payload_size, consumed);
        let serial_types: Vec<RecordSerialType> = (0..0)
            .into_iter()
            .map(|_| RecordSerialType::from(buf.get_u8()))
            .collect();

        let payload: Vec<RecordValue> = serial_types
            .iter()
            .map(|st| match *st {
                RecordSerialType::Null => RecordValue::Null,
                RecordSerialType::I8 => RecordValue::I8(buf.get_i8()),
                RecordSerialType::I16 => RecordValue::I16(buf.get_i16()),
                RecordSerialType::I24 => RecordValue::I24(buf.get_i32()),
                RecordSerialType::I32 => RecordValue::I32(buf.get_i32()),
                RecordSerialType::I48 => RecordValue::I48(buf.get_i64()),
                RecordSerialType::I64 => RecordValue::I64(buf.get_i64()),
                RecordSerialType::F64 => RecordValue::F64(buf.get_f64()),
                RecordSerialType::Bool => {
                    let value = buf.get_u8();
                    let bool = if value == 0 { false } else { true };

                    RecordValue::Bool(bool)
                }
                RecordSerialType::Blob(size) => {
                    let blob = (0..size).into_iter().map(|_| buf.get_u8()).collect();
                    RecordValue::Blob(blob)
                }
                RecordSerialType::String(size) => {
                    let data = (0..size).into_iter().map(|_| buf.get_u8()).collect();
                    RecordValue::String(String::from_utf8(data).expect("invalid string format"))
                }
                RecordSerialType::Internal => RecordValue::Null,
            })
            .collect();

        Self {
            record_header_size: 0,
            row_id: 0,
            serial_types,
            payload,
            overflow_page: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum RecordValue {
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

#[derive(Debug, PartialEq)]
enum RecordSerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Bool,
    Blob(usize),
    String(usize),
    Internal,
}

impl From<u8> for RecordSerialType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::F64,
            8 | 9 => Self::Bool,
            10 | 11 => Self::Internal,
            value if value >= 12 && value % 2 == 0 => Self::Blob((usize::from(value) - 12) / 2),
            value if value >= 13 && value % 2 != 0 => Self::String((usize::from(value) - 13) / 2),
            _ => panic!("invalid value for record serial type: {value}"),
        }
    }
}
