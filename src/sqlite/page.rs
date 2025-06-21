use bytes::{Buf, Bytes};

const MAX_VARINT_SIZE: u8 = 9;

fn parse_varint(buf: &[u8]) -> Option<(u64, u8)> {
    // Varints are 9 bytes max
    let mut buf = Bytes::copy_from_slice(&buf[..usize::from(MAX_VARINT_SIZE)]);

    let mut varint: u64 = 0;
    for offset in 0..9 {
        let n = buf.get_u8();
        if offset == 8 {
            varint |= (n as u64) << (7 * offset);
            return Some((varint, MAX_VARINT_SIZE));
        } else {
            varint |= ((n & 0x7f) as u64) << (7 * offset);
            if n & 0x80 == 0 {
                return Some((varint, offset + 1));
            }
        }
    }

    None
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BTreePageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
}

impl From<u8> for BTreePageType {
    fn from(value: u8) -> Self {
        match value {
            2 => Self::InteriorIndex,
            5 => Self::InteriorTable,
            10 => Self::LeafIndex,
            13 => Self::LeafTable,
            _ => panic!("unsupported value for BTreePageType: {value}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    pub page_type: BTreePageType,
    pub first_freeblock_offset: u16,
    pub total_cells: u16,
    pub cell_content_offset: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct BTreePage {
    pub header: BTreePageHeader,
    rest: Vec<u8>,
}

impl BTreePage {
    pub fn new(mut buf: &[u8]) -> Self {
        let page_type = BTreePageType::from(buf.get_u8());
        let header = BTreePageHeader {
            page_type,
            first_freeblock_offset: buf.get_u16(),
            total_cells: buf.get_u16(),
            cell_content_offset: {
                let value = buf.get_u16();
                if value == 0 {
                    u16::MAX
                } else {
                    value
                }
            },
            fragmented_free_bytes: buf.get_u8(),
            rightmost_pointer: if page_type == BTreePageType::InteriorIndex
                || page_type == BTreePageType::InteriorTable
            {
                Some(buf.get_u32())
            } else {
                None
            },
        };

        let total_cells = usize::from(header.total_cells);
        dbg!(total_cells);
        let cell_pointers: Vec<u16> = (0..total_cells)
            .into_iter()
            .map(|_| buf.get_u16())
            .collect();
        dbg!(&cell_pointers);

        // Cell pointer value is the offset (offset - 100 if its the first page)

        Self {
            header,
            rest: buf.to_vec(),
        }
    }
}
