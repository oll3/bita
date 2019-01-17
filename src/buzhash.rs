#![cfg_attr(feature = "cargo-clippy", allow(clippy::unreadable_literal))]
static BUZHASH_TABLE: &'static [u32] = &[
    0xa40cc360, 0xbb785af8, 0x32c790bc, 0x6c64cd34, 0x83b4aa73, 0x36b691a5, 0x4631ad79, 0x5e49d231,
    0xcab22600, 0x6d45bfdf, 0xcd26dfc8, 0xf2e63bec, 0x18cb0c69, 0x817876fd, 0xb8c88ca8, 0xfabcc7d3,
    0x21211be3, 0x7bbd8a4b, 0xa7bc508f, 0xcf35be6, 0x5bb76463, 0x900cc253, 0xa1bf6569, 0xb5fc01ff,
    0xd9d8d709, 0xf2fdd75b, 0x8cf7fdcc, 0x7a24b10f, 0xc2dfa4ea, 0xaec2a258, 0xb8e51edf, 0x81944196,
    0x1040fd08, 0x728857d5, 0xd4ec5341, 0x2e547382, 0x337cab71, 0x792f8dac, 0x6932136a, 0x211f28b,
    0xeba45b99, 0x47a35907, 0x77271fb4, 0xbc09c6fb, 0x2d4eaf65, 0xfd7b53c0, 0xed4c2634, 0x54c8155d,
    0x8078f561, 0xd87a7a9, 0x4e75f462, 0x251c62a7, 0xb038d1a8, 0x57d57b20, 0x500b6495, 0xee5fbf54,
    0x97bb0e7c, 0xfe5ca9ad, 0xa91746dd, 0x160b7122, 0xafc5ceee, 0x7d850b16, 0x15c28cc5, 0x655bc438,
    0x6ed4a616, 0x1a0e18f, 0xaae23461, 0xe387536b, 0x8b2470b1, 0x92be2ce1, 0xb18b3370, 0x803d5e25,
    0x8a94f196, 0xcd225534, 0xee4f24dc, 0x51749bfb, 0xcf068d2e, 0xc859a603, 0xfc12373b, 0x8e422af2,
    0xf28be938, 0x7619c3f8, 0xea77dc6b, 0x5006f1e4, 0x297c8237, 0x445bd977, 0x67cad7ed, 0xefde9644,
    0x626bed36, 0x276970b, 0x1e699630, 0x373ee5db, 0xe83a37b3, 0x2b3938bc, 0xb2132f15, 0x66e60490,
    0x72e4e7ef, 0x8c4c2912, 0x6f7aab32, 0xc8f2eb65, 0x9b1c772b, 0xf48fb997, 0xd7b70417, 0x5a1476bd,
    0xfe547592, 0x82c7e59c, 0xede3935b, 0x8954101, 0x79f75116, 0x18a6b281, 0x90d2f6e6, 0x9c0139d9,
    0xba4a4d22, 0xfa3ab4db, 0xfc18be27, 0x47f414d, 0xba7ff7b4, 0xcc2b1b72, 0x756145da, 0xf12ff6c0,
    0xae78b289, 0x55935d1d, 0xfd849c3e, 0xae586a4f, 0x8948b6f5, 0xa079d886, 0xd7f5a118, 0x9ca03cf1,
    0xc500e85d, 0x8ce73a6c, 0x6725bb68, 0xfdb46ea2, 0x2ec367c4, 0x8fcdee2f, 0x21b13487, 0x67b41d6b,
    0xe662a354, 0x9cad336e, 0x8b284ff5, 0x1cf3c8b3, 0x6de04fd1, 0xf8ed762d, 0xc30471d4, 0x2c4ecebc,
    0x7211a0b8, 0xa34ee19c, 0x47b4bb79, 0x90777b6, 0x77f02513, 0x7cba4a74, 0xd87c18d3, 0xad8b9c5e,
    0x430c3f91, 0x72122ed, 0xc489ce87, 0x5798ec5, 0xc21e987b, 0x4eb62176, 0x8dfafefe, 0xfaeebadc,
    0x6fdc0b01, 0x60ebe6ee, 0x4244a970, 0x22d97939, 0x7ae88397, 0x76434679, 0xf8a624b6, 0x45ebdb60,
    0x68808c40, 0x628db2cb, 0xb6e16fe, 0x8fbd2c92, 0xf5737f03, 0x562d3fc5, 0xd3306769, 0x97bd2f93,
    0x7a5312e6, 0x2065be8, 0xb0c0a57, 0x80ad5f0e, 0x30e049bd, 0x5cd85bbc, 0xf981af4b, 0xbc18728f,
    0x85ce9d03, 0xc2c97922, 0xbedb47dd, 0x420f23c2, 0x40b504d3, 0x9d28e6fe, 0x7549831f, 0xa9668219,
    0xd24b01ae, 0x8e1c6dd7, 0xe5c94cf8, 0x34a91576, 0x48a005e5, 0x5c671015, 0xef4797c5, 0xd7476c01,
    0x37c339e7, 0xae017bab, 0xb5d8e74f, 0x50c5fc5b, 0x3b14aa64, 0xfc693cb1, 0x9cc65b82, 0x407c538f,
    0x71b2a28a, 0x297e5b01, 0xa3f7007e, 0x742f937, 0xe05c4998, 0x6fd8e28f, 0x65036a03, 0xdc7f1a37,
    0x4b73b787, 0xf4fa5820, 0x85097266, 0x567d4e36, 0xdcc0aa2f, 0x5ff8969b, 0x464620a0, 0x8c694162,
    0x868a341a, 0x7bd4619e, 0x2c90aa84, 0x25668b2, 0xca4d0465, 0xcb579718, 0xdc7aa0eb, 0xa3605310,
    0x7a7910c6, 0x6a8ecffc, 0x6598058a, 0xfc5fb823, 0xbc3ef53a, 0xf0d09546, 0xeb210009, 0xf58cb960,
    0xbecbb337, 0xb84b6459, 0x4ebf437d, 0x85610fd2, 0x16e79f2f, 0x849cce5, 0x77c3bd48, 0x955719e2,
    0x87abd4a7, 0x2799ae4f, 0x3b80cac, 0xd56e7604, 0x8b07ed07, 0x944552c5, 0x5b93e058, 0x8fbd2c92,
];

#[derive(Clone)]
pub struct BuzHash {
    buf: Vec<u32>,
    index: usize,
    window: usize,
    hash_sum: u32,
    buzhash_table: Vec<u32>,
    window_full: bool,
    last_input: u8,
    repeated_input: usize,
}

impl BuzHash {
    pub fn new(window: usize, seed: u32) -> Self {
        BuzHash {
            index: 0,
            buf: vec![0; window],
            window,
            hash_sum: 0,
            buzhash_table: Self::generate_seeded_table(seed),
            window_full: false,
            last_input: 0,
            repeated_input: 0,
        }
    }

    fn generate_seeded_table(seed: u32) -> Vec<u32> {
        BUZHASH_TABLE.iter().map(|x| x ^ seed).collect()
    }

    // Hash is valid when window is full
    pub fn valid(&self) -> bool {
        self.window_full
    }

    pub fn window_size(&self) -> usize {
        self.window
    }

    pub fn init(&mut self, in_val: u8) {
        if !self.window_full {
            let in_val = self.buzhash_table[in_val as usize];
            // Initialize sequence until window is full
            let shift = self.window - (self.index + 1);
            self.hash_sum ^= in_val.rotate_left(shift as u32);
            self.window_full = self.index >= (self.window - 1);
            self.buf[self.index] = in_val;
            self.index += 1;
            if self.index >= self.window {
                self.index = 0;
            }
        }
    }

    // Push and process a byte
    pub fn input(&mut self, in_val: u8) {
        // If the buzhash window is full of the same value then there is no
        // need pushing another one of the same as it won't change the hash.
        if in_val == self.last_input {
            self.repeated_input += 1;
        } else {
            self.repeated_input = 0;
            self.last_input = in_val;
        }
        if self.repeated_input < self.window {
            let in_val = self.buzhash_table[in_val as usize];

            let out_val = self.buf[self.index];
            self.hash_sum =
                self.hash_sum.rotate_left(1) ^ out_val.rotate_left(self.window as u32) ^ in_val;
            self.buf[self.index] = in_val;
            self.index += 1;
            if self.index >= self.window {
                self.index = 0;
            }
        }
    }

    // Get current hash sum
    pub fn sum(&self) -> u32 {
        self.hash_sum
    }
}

#[cfg(test)]
mod tests {
    use crate::buzhash::BuzHash;
    #[test]
    fn equal_sums_for_equal_range() {
        let window_size = 8;
        let mut h = BuzHash::new(window_size, 0x10324195);
        let data1 = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        ];
        let data2 = vec![
            1, 99, 99, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        ];
        let sums1: Vec<u32> = data1
            .iter()
            .map(|v| {
                h.input(*v);
                h.sum()
            })
            .collect();

        let sums2: Vec<u32> = data2
            .iter()
            .map(|v| {
                h.input(*v);
                h.sum()
            })
            .collect();

        assert_eq!(sums1[11..], sums2[11..]);
    }
}
