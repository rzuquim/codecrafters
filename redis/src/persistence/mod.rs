pub mod in_mem;

pub trait Store {
    fn set(&mut self, key: String, value: String);
    fn set_expiring(&mut self, key: String, value: String, expiry_in_millis: u32);
    fn get(&self, key: &str) -> Option<String>;
}
