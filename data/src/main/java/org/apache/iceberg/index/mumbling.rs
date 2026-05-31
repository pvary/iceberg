use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, PartialEq, Eq, Debug)]
enum MiniContainer {
    Dense([u64; 4]),
    Sparse([u8; 31]),
}

impl MiniContainer {
    fn convert_to_dense(&mut self) {
        if let MiniContainer::Sparse(sparse_positions) = self {
            let mut bit_storage = [0u64; 4];
            for position in sparse_positions {
                util::dense_insert(*position, &mut bit_storage);
            }
            *self = MiniContainer::Dense(bit_storage);
        }
    }

    fn is_sparse(&self) -> bool {
        match self {
            MiniContainer::Sparse(_) => true,
            _ => false,
        }
    }

    fn is_dense(&self) -> bool {
        match self {
            MiniContainer::Dense(_) => true,
            _ => false,
        }
    }

    fn has_unset_position(&self) -> bool {
        // when a container is full the count cannot represent the actual values
        // this is used to determine whether count = 255 means count is 255 or 256
        if let MiniContainer::Dense(dense_positions) = self {
            for positions in dense_positions {
                if *positions < u64::MAX {
                    return true;
                }
            }
        }

        false
    }

    fn cardinality(&self, count: u8) -> u16 {
        if count < 255 {
            count as u16
        } else if self.has_unset_position() {
            255
        } else {
            256
        }
    }

    fn contains(&self, value: u8, size: u8) -> bool {
        match self {
            MiniContainer::Sparse(sparse_positions) => {
                util::sparse_contains(value, sparse_positions, size)
            }
            MiniContainer::Dense(dense_positions) => util::dense_contains(value, dense_positions),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MumblingBitmap {
    sizes: [u8; 256],
    containers: [MiniContainer; 256],
}

mod util {
    #[inline]
    pub(super) fn sparse_contains(value: u8, sparse_positions: &[u8; 31], count: u8) -> bool {
        for index in 0..count as usize {
            if value == sparse_positions[index] {
                return true;
            } else if value < sparse_positions[index] {
                return false;
            }
        }

        false
    }

    #[inline]
    pub(super) fn sparse_insert(
        value: u8,
        sparse_positions: &mut [u8; 31],
        count: &mut u8,
    ) -> bool {
        assert![*count < 31, "Cannot add position, already full"];

        let insert_at = find_insert_index(value, sparse_positions, *count);
        if insert_at == *count as usize || value != sparse_positions[insert_at] {
            if insert_at != *count as usize {
                move_tail(insert_at, sparse_positions, *count)
            }

            sparse_positions[insert_at] = value;
            *count += 1;

            true
        } else {
            false
        }
    }

    #[inline]
    fn find_insert_index(value: u8, sparse_positions: &[u8; 31], count: u8) -> usize {
        let mut ind = 0;
        while ind < count as usize && value > sparse_positions[ind] {
            ind += 1;
        }

        ind
    }

    #[inline]
    fn move_tail(index: usize, sparse_positions: &mut [u8; 31], count: u8) {
        let mut pos = count as usize;
        while pos > index {
            sparse_positions[pos] = sparse_positions[pos - 1];
            pos -= 1;
        }
    }

    #[inline]
    pub(super) fn dense_contains(value: u8, dense_positions: &[u64; 4]) -> bool {
        let pattern = bit_pattern(value);
        let positions = dense_positions[(value / 64) as usize];
        (positions & pattern) == pattern
    }

    #[inline]
    pub(super) fn dense_insert(value: u8, dense_positions: &mut [u64; 4]) -> bool {
        let pattern = bit_pattern(value);
        let index = (value / 64) as usize;
        let is_set = dense_positions[index] & pattern != pattern;
        dense_positions[index] |= pattern;
        is_set
    }

    #[inline]
    fn bit_pattern(value: u8) -> u64 {
        1u64 << (63 - (value & 0b111111))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_sparse_container_empty_insert() {
            let mut sparse_container = [0u8; 31];
            let mut count = 0;

            assert!(!sparse_contains(0, &sparse_container, count));
            sparse_insert(0, &mut sparse_container, &mut count);
            assert!(sparse_contains(0, &sparse_container, count));

            assert_eq!(sparse_container[0], 0);
            for pos in 1..31 {
                assert_eq!(sparse_container[pos], 0);
            }
            assert_eq!(1, count)
        }

        #[test]
        fn test_sparse_container_insert_ordering() {
            let mut sparse_container = [0u8; 31];
            let mut count = 0;

            // each insert will be at the beginning
            for pos in 0..31 {
                sparse_insert(30 - pos, &mut sparse_container, &mut count);
            }

            for pos in 0..31u8 {
                assert_eq!(sparse_container[pos as usize], pos);
            }

            assert_eq!(count, 31);
        }

        #[test]
        fn test_sparse_insert_dedup() {
            let mut sparse_container = [0u8; 31];
            let mut count = 0;

            assert_eq!(sparse_insert(5, &mut sparse_container, &mut count), true);
            assert_eq!(sparse_insert(6, &mut sparse_container, &mut count), true);
            assert_eq!(sparse_insert(5, &mut sparse_container, &mut count), false);
            assert_eq!(sparse_insert(4, &mut sparse_container, &mut count), true);
            assert_eq!(sparse_insert(6, &mut sparse_container, &mut count), false);

            let mut expected = [0u8; 31];
            expected[0] = 4;
            expected[1] = 5;
            expected[2] = 6;

            assert_eq!(expected, sparse_container);
            assert_eq!(count, 3);
        }

        #[test]
        fn test_sparse_container_inclusion() {
            let mut sparse_container = [0u8; 31];
            let mut count = 0;

            // ensure value are not contained unless inserted
            assert!(!sparse_contains(0, &sparse_container, count));
            assert!(!sparse_contains(5, &sparse_container, count));
            assert!(!sparse_contains(255, &sparse_container, count));

            sparse_insert(255, &mut sparse_container, &mut count);
            assert!(!sparse_contains(0, &sparse_container, count));
            assert!(!sparse_contains(5, &sparse_container, count));
            assert!(sparse_contains(255, &sparse_container, count));

            sparse_insert(0, &mut sparse_container, &mut count);
            assert!(sparse_contains(0, &sparse_container, count));
            assert!(!sparse_contains(5, &sparse_container, count));
            assert!(sparse_contains(255, &sparse_container, count));

            sparse_insert(5, &mut sparse_container, &mut count);
            for pos in 0..=255u8 {
                assert_eq!(
                    sparse_contains(pos, &sparse_container, count),
                    pos == 0 || pos == 5 || pos == 255
                );
            }
        }

        #[test]
        fn test_dense_insert_dedup() {
            let mut dense_container = [0u64; 4];

            assert_eq!(dense_insert(5, &mut dense_container), true);
            assert_eq!(dense_insert(6, &mut dense_container), true);
            assert_eq!(dense_insert(5, &mut dense_container), false);
            assert_eq!(dense_insert(4, &mut dense_container), true);
            assert_eq!(dense_insert(6, &mut dense_container), false);
            assert_eq!(dense_insert(255, &mut dense_container), true);

            let mut expected = [0u64; 4];
            expected[0] = 0b00001110_00000000_00000000_00000000_00000000_00000000_00000000_00000000;
            expected[3] = 0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001;

            assert_eq!(expected, dense_container);
        }

        #[test]
        fn test_dense_container_inclusion() {
            let mut dense_container = [0u64; 4];

            // ensure value are not contained unless inserted
            assert!(!dense_contains(0, &dense_container));
            assert!(!dense_contains(5, &dense_container));
            assert!(!dense_contains(255, &dense_container));

            dense_insert(255, &mut dense_container);
            assert!(!dense_contains(0, &dense_container));
            assert!(!dense_contains(5, &dense_container));
            assert!(dense_contains(255, &dense_container));

            dense_insert(0, &mut dense_container);
            assert!(dense_contains(0, &dense_container));
            assert!(!dense_contains(5, &dense_container));
            assert!(dense_contains(255, &dense_container));

            dense_insert(5, &mut dense_container);
            for pos in 0..=255u8 {
                assert_eq!(
                    dense_contains(pos, &dense_container),
                    pos == 0 || pos == 5 || pos == 255
                );
            }
        }
    }
}

const EMPTY_CONTAINER: MiniContainer = MiniContainer::Sparse([0u8; 31]);

impl MumblingBitmap {
    pub fn new() -> MumblingBitmap {
        MumblingBitmap {
            sizes: [0u8; 256],
            containers: [EMPTY_CONTAINER; 256],
        }
    }

    pub fn is_set(&self, pos: u16) -> bool {
        let key = (pos >> 8) as u8 as usize;
        let size = self.sizes[key];
        if size > 0 {
            self.containers[key].contains(pos as u8, size)
        } else {
            false
        }
    }

    pub fn set(&mut self, pos: u16) -> bool {
        let key = (pos >> 8) as u8 as usize;
        let val = pos as u8;
        let size = self.sizes[key];

        let mut added = false;
        if size < 31 {
            // the mini-container will still be sparse
            if let MiniContainer::Sparse(sparse_positions) = &mut self.containers[key] {
                added = util::sparse_insert(val, sparse_positions, &mut self.sizes[key]);
            } else {
                // TODO: detect the density and update the count
                panic!("Found dense mini-container for {} values", size)
            }
        } else {
            if size == 31 && !self.containers[key].contains(val, size) {
                // convert the mini-container to dense
                self.containers[key].convert_to_dense();
            }

            if let MiniContainer::Dense(dense_positions) = &mut self.containers[key] {
                added = util::dense_insert(val, dense_positions);
                if added && self.sizes[key] < 255 {
                    self.sizes[key] += 1;
                }
            }
        }

        added
    }

    pub fn cardinality(&self) -> u16 {
        let mut total = 0u16;
        for ind in 0..256 {
            total += self.containers[ind].cardinality(self.sizes[ind]);
        }

        total
    }

    pub fn size(&self) -> usize {
        let mut total = 256; // size bytes
        for size in self.sizes {
            total += if size > 31 { 32 } else { size as usize };
        }

        total
    }

    pub fn serialize(&self) -> Bytes {
        // TODO: should this consume the value?
        let mut buffer = BytesMut::with_capacity(self.size());
        buffer.put(self.sizes.as_ref());
        for ind in 0..256 {
            let size = self.sizes[ind];
            if size > 0 {
                match self.containers[ind] {
                    MiniContainer::Sparse(sparse_positions) => {
                        assert!(size < 32);
                        buffer.put(&sparse_positions[0..size as usize])
                    }
                    MiniContainer::Dense(dense_positions) => {
                        assert!(size > 31);
                        for positions in dense_positions {
                            buffer.put_u64(positions)
                        }
                    }
                }
            }
        }

        buffer.freeze()
    }

    pub fn deserialize(buffer: &mut Bytes) -> Self {
        let mut bitmap = MumblingBitmap::new();
        buffer.copy_to_slice(bitmap.sizes.as_mut());

        for ind in 0..256 {
            let size = bitmap.sizes[ind];
            if size > 31 {
                let mut dense_positions = [0u64; 4];
                for i in 0..4 {
                    dense_positions[i] = buffer.get_u64();
                }
                bitmap.containers[ind] = MiniContainer::Dense(dense_positions);
            } else if size > 0 {
                if let MiniContainer::Sparse(sparse_positions) = &mut bitmap.containers[ind] {
                    buffer.copy_to_slice(&mut sparse_positions[0..size as usize]);
                }
            }
        }

        bitmap
    }
}

fn search(value: u8, positions: [u8; 32], count: usize) -> usize {
    let mut low = 0_usize;
    let mut high = count - 1;
    while low <= high {
        let mid = (low + high) >> 1;
        let pos = positions[mid];
        if pos == value {
            return mid;
        } else if pos < value {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    count
}

fn is_bit_set_bytes(value: u16, dense_positions: [u8; 32]) -> bool {
    let byte_index = ((value >> 4) & 0b1111) as usize;
    let positions = dense_positions[byte_index];
    let shift = 8 - (value & 0b1111);
    ((positions >> shift) & 0b1) != 0
}

#[cfg(test)]
mod tests {
    use crate::MumblingBitmap;
    use bytes::{Buf, BufMut, BytesMut};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::io::{Read, Write};

    #[test]
    fn test_simple_insert() {
        let mut bitmap = MumblingBitmap::new();
        bitmap.set(0);
        bitmap.set(255);
        bitmap.set(65535);

        assert_eq!(bitmap.is_set(0), true);
        assert_eq!(bitmap.is_set(1), false);
        assert_eq!(bitmap.is_set(255), true);
        assert_eq!(bitmap.is_set(256), false);
        assert_eq!(bitmap.is_set(65535), true);
    }

    #[test]
    fn test_sparse_to_dense_conversion() {
        let mut bitmap = MumblingBitmap::new();

        for pos in 0..31 {
            assert_eq!(bitmap.is_set(pos), false);
            bitmap.set(pos);
            assert_eq!(bitmap.is_set(pos), true);
        }

        assert_eq!(bitmap.is_set(32), false);
        assert!(bitmap.containers[0].is_sparse());
        assert!(!bitmap.containers[0].is_dense());

        bitmap.set(32);

        assert_eq!(bitmap.is_set(32), true);
        assert!(!bitmap.containers[0].is_sparse());
        assert!(bitmap.containers[0].is_dense());

        for pos in 0..31 {
            assert_eq!(bitmap.is_set(pos), true);
        }
    }

    #[test]
    fn test_cardinality() {
        let mut bitmap = MumblingBitmap::new();

        for num in 0..254 {
            assert!(bitmap.set(num));
            assert_eq!(bitmap.cardinality(), num + 1);
        }

        assert!(bitmap.set(254));
        assert_eq!(bitmap.cardinality(), 255);

        assert!(bitmap.set(255));
        assert_eq!(bitmap.cardinality(), 256);
    }

    #[test]
    fn test_random_values() {
        let mut rng = StdRng::seed_from_u64(44);

        let mut bitmap = MumblingBitmap::new();
        let mut count = 0;
        for _ in 0..50_000 {
            if bitmap.set(rng.random()) {
                count += 1;
            }
        }

        let mut rng = StdRng::seed_from_u64(44);
        for _ in 0..50_000 {
            assert_eq!(bitmap.is_set(rng.random()), true);
        }

        assert_eq!(bitmap.cardinality(), count);
    }

    #[test]
    fn test_serialization_round_trip() {
        let mut rng = StdRng::seed_from_u64(728);

        let mut bitmap = MumblingBitmap::new();
        let mut count = 0;
        for _ in 0..50_000 {
            if bitmap.set(rng.random()) {
                count += 1;
            }
        }

        let mut serialized = bitmap.serialize();
        assert_eq!(bitmap.size(), serialized.len());

        let new_bitmap = MumblingBitmap::deserialize(&mut serialized);

        let mut rng = StdRng::seed_from_u64(728);
        for _ in 0..50_000 {
            assert_eq!(new_bitmap.is_set(rng.random()), true);
        }

        assert_eq!(new_bitmap.cardinality(), count);
    }

    #[test]
    fn test_1mb_buffer() -> Result<(), std::io::Error> {
        for card in [
            100, 500, 1000, 2500, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000,
        ] {
            let path = format!("/tmp/mumbling_zstd_50000-{}.bin", card);
            let mut zstd_stream = zstd::Encoder::new(std::fs::File::create(&path)?, 9)?;
            let mut size_buffer = BytesMut::with_capacity(4);
            let mut rng = StdRng::seed_from_u64(728);
            let mut iterations = 0;
            let mut len = 0;
            let mut bitsets = Vec::new();
            let mut bitmaps = Vec::new();
            while len < 1024 * 1024 {
                // create a new bitmap to write
                let mut bitmap = MumblingBitmap::new();
                let mut bitset = Vec::new();
                while bitmap.cardinality() < card {
                    let r: u16 = rng.random();
                    bitset.push(r % 50_000_u16);
                    bitmap.set(r % 50_000_u16);
                }

                bitsets.push(bitset);
                bitmaps.push(bitmap.clone());

                size_buffer.clear();
                size_buffer.put_u32_le(bitmap.size() as u32);
                len += bitmap.size() + 4;
                zstd_stream.write_all(size_buffer.as_ref())?;
                zstd_stream.write_all(bitmap.serialize().as_ref())?;
                iterations += 1;
            }
            zstd_stream.finish()?;

            println!(
                "cardinality: {}, iterations: {}, uncompressed: {}",
                card, iterations, len
            );

            // validation
            let mut zstd_input = zstd::Decoder::new(std::fs::File::open(&path)?)?;
            size_buffer.resize(4, 0);
            let mut bytes_read = zstd_input.read_exact(size_buffer.as_mut());
            let mut i = 0;
            while bytes_read.is_ok() {
                let size = size_buffer.get_u32_le() as usize;
                size_buffer.clear();
                let mut buffer = BytesMut::with_capacity(size);
                buffer.resize(size, 0);
                zstd_input.read_exact(buffer.as_mut())?;
                let original = bitmaps.get(i).expect("should exist");
                let bitmap = MumblingBitmap::deserialize(&mut buffer.freeze());
                let bitset = bitsets
                    .get(i)
                    .expect(&format!("Should contain a position set for bitmap {}", i));

                assert_eq!(
                    *original,
                    MumblingBitmap::deserialize(&mut original.serialize())
                );

                for pos in bitset {
                    assert_eq!(bitmap.is_set(*pos % 50_000_u16), true);
                }

                i += 1;
                size_buffer.resize(4, 0);
                bytes_read = zstd_input.read_exact(size_buffer.as_mut());
            }
        }

        Ok(())
    }
}