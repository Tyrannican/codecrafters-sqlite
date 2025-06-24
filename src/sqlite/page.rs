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
    rest: Vec<u8>,
}

impl BTreePage {
    pub fn new(mut buf: &[u8], page_no: usize) -> Self {
        let buf_len = buf.len();
        let page_type = BTreePageType::from(buf.get_u8());
        let header = BTreePageHeader {
            page_type,
            first_freeblock_offset: buf.get_u16(),
            total_cells: buf.get_u16(),
            cell_content_offset: {
                let value = buf.get_u16();
                dbg!(value);
                if value == 0 {
                    u16::MAX
                } else if page_no == 0 {
                    value - HEADER_SIZE as u16
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

        dbg!(&header);
        dbg!(&buf.len());

        let total_cells = usize::from(header.total_cells);
        let cell_pointers: Vec<usize> = (0..total_cells)
            .into_iter()
            .map(|_| {
                let value = if page_no == 0 {
                    buf.get_u16() - HEADER_SIZE as u16
                } else {
                    buf.get_u16()
                };

                usize::from(value - (buf_len - buf.remaining()) as u16)
            })
            .collect();
        dbg!(&cell_pointers);
        println!("{:x?}", &buf[cell_pointers[2]..cell_pointers[2] + 10]);

        // println!("{:x?}", &buf[3665..3665 + 10]);
        // let cell = DatabaseCell::BTreeLeafCell(BTreeLeafCell::new(
        //     &buf[usize::from(header.cell_content_offset)..],
        // ));

        Self {
            header,
            page_no,
            cells,
            rest: buf.to_vec(),
        }
    }
}
