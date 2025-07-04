use bytes::Buf;

use super::cell::{BTreeLeafCell, DatabaseCell};
use super::HEADER_SIZE;

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
            BTreePageType::LeafTable | BTreePageType::LeafIndex => 8,
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => 12,
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
            rightmost_pointer: if page_type == BTreePageType::InteriorIndex
                || page_type == BTreePageType::InteriorTable
            {
                Some(header_bytes.get_u32())
            } else {
                None
            },
        };

        let total_cells = usize::from(header.total_cells);
        let mut cell_pointer_buf =
            &buf[header_offset..header_offset + (2 * usize::from(header.total_cells))];

        let cells: Vec<DatabaseCell> = (0..total_cells)
            .into_iter()
            .map(|_| {
                let offset = usize::from(cell_pointer_buf.get_u16());
                let offset = if page_no == 0 {
                    offset - HEADER_SIZE
                } else {
                    offset
                };

                // TODO: Deal with the others as
                match page_type {
                    BTreePageType::LeafTable => {
                        DatabaseCell::BTreeLeafCell(BTreeLeafCell::new(&buf[offset..]))
                    }
                    other => todo!("when the time is right: {other:#?}"),
                }
            })
            .collect();

        Self {
            header,
            page_no,
            cells,
        }
    }

    pub fn fetch_row(&self, row: usize) -> &DatabaseCell {
        assert!(row < self.cells.len());
        &self.cells[row]
    }

    pub fn count(&self) -> usize {
        self.cells.len()
    }
}
