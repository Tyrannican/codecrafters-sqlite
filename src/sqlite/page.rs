use bytes::Buf;

use super::cell::{DatabaseCell, IndexLeafCell, InteriorIndexCell, InteriorTableCell, LeafCell};
use super::HEADER_SIZE;

const LEAF_OFFSET: usize = 8;
const INTERIOR_OFFSET: usize = 12;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BTreePageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
}

impl std::fmt::Display for BTreePageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InteriorIndex => write!(f, "Interior Index Page"),
            Self::InteriorTable => write!(f, "Interior Table Page"),
            Self::LeafIndex => write!(f, "Index Leaf Page"),
            Self::LeafTable => write!(f, "Table Leaf Page"),
        }
    }
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

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    pub page_type: BTreePageType,
    pub first_freeblock_offset: u16,
    pub total_cells: u16,
    pub cell_content_offset: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BTreePage {
    page_no: usize,
    pub header: BTreePageHeader,
    pub cells: Vec<DatabaseCell>,
}

impl BTreePage {
    pub fn new(buf: &[u8], page_no: usize) -> Self {
        let page_type = BTreePageType::from(buf[0]);
        let header_offset = match page_type {
            BTreePageType::LeafTable | BTreePageType::LeafIndex => LEAF_OFFSET,
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => INTERIOR_OFFSET,
        };
        let mut header_bytes = &buf[1..header_offset];

        let header = BTreePageHeader {
            page_type,
            first_freeblock_offset: header_bytes.get_u16(),
            total_cells: header_bytes.get_u16(),
            cell_content_offset: {
                let value = header_bytes.get_u16();
                if value == 0 {
                    u16::MAX
                } else if page_no == 0 {
                    value - HEADER_SIZE as u16
                } else {
                    value
                }
            },
            fragmented_free_bytes: header_bytes.get_u8(),
            rightmost_pointer: match page_type {
                BTreePageType::InteriorTable | BTreePageType::InteriorIndex => {
                    let page_number = header_bytes.get_u32();
                    Some(page_number - 1)
                }
                _ => None,
            },
        };

        let total_cells = usize::from(header.total_cells);
        let mut cell_pointer_buf =
            &buf[header_offset..header_offset + (2 * usize::from(header.total_cells))];

        let cells: Vec<DatabaseCell> = (0..total_cells)
            .map(|_| {
                let offset = usize::from(cell_pointer_buf.get_u16());
                let offset = if page_no == 0 {
                    offset - HEADER_SIZE
                } else {
                    offset
                };

                let cell_buf = &buf[offset..];
                match page_type {
                    BTreePageType::LeafTable => DatabaseCell::Leaf(LeafCell::new(cell_buf)),
                    BTreePageType::InteriorTable => {
                        DatabaseCell::InteriorTable(InteriorTableCell::new(cell_buf))
                    }
                    BTreePageType::InteriorIndex => {
                        DatabaseCell::InteriorIndex(InteriorIndexCell::new(cell_buf))
                    }
                    BTreePageType::LeafIndex => {
                        DatabaseCell::IndexLeaf(IndexLeafCell::new(cell_buf))
                    }
                }
            })
            .collect();

        Self {
            header,
            page_no,
            cells,
        }
    }

    pub fn page_type(&self) -> BTreePageType {
        self.header.page_type
    }

    pub fn right_page_pointer(&self) -> Option<u32> {
        self.header.rightmost_pointer
    }

    pub fn count(&self) -> usize {
        self.cells.len()
    }
}
