#[derive(Debug, Default)]
pub struct SerialId(u64);

impl SerialId {
    pub fn new() -> SerialId {
        Self::default()
    }

    pub fn next(&mut self) -> u64 {
        let id = self.0;
        self.0 += 1;
        id
    }
}