#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let elements = self.offsets.len() as u16;
        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() + 2);
        for data in self.data.clone() {
            buf.put_u8(data);
        }
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(elements);
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        if data_len < 2 {
            return Self {
                data: vec![],
                offsets: vec![],
            };
        }
        let mut bytes = Bytes::from(data.to_vec());
        let elements = ((bytes[data_len - 2] as u16) << 8) | bytes[data_len - 1] as u16;
        let mut data = vec![];
        for _ in 0..elements {
            let kl_high = bytes.get_u8();
            let kl_low = bytes.get_u8();
            let kl = ((kl_high as u16) << 8) | kl_low as u16;

            let key = bytes.split_to(kl as usize);

            let vl_high = bytes.get_u8();
            let vl_low = bytes.get_u8();
            let vl = ((vl_high as u16) << 8) | vl_low as u16;

            let value = bytes.split_to(vl as usize);

            let mut curr_data = vec![kl_high, kl_low];
            curr_data.extend(key.to_vec());
            curr_data.push(vl_high);
            curr_data.push(vl_low);
            curr_data.extend(value.to_vec());
            data.extend(curr_data);
        }
        let mut offsets = vec![];
        for _ in 0..elements {
            offsets.push(bytes.get_u16());
        }

        Self { data, offsets }
    }
}
