use std::time::{SystemTime, UNIX_EPOCH};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::Store;

#[derive(Clone)]
pub struct InMemStore {
    store: Arc<Mutex<HashMap<String, Value>>>,
}

#[derive(Clone)]
pub struct Value {
    data: String,
    expires_at: Option<u128>,
}

impl InMemStore {
    pub fn new() -> Self {
        return InMemStore {
            store: Arc::new(Mutex::new(HashMap::new())),
        };
    }
}

impl Store for InMemStore {
    fn set(&mut self, key: String, value: String) {
        let mut store = self.store.lock().unwrap();
        store.insert(
            key,
            Value {
                data: value,
                expires_at: None,
            },
        );
    }

    fn set_expiring(&mut self, key: String, value: String, expiry_in_millis: u32) {
        let timestamp = current_timestamp();
        let expires_at = timestamp + expiry_in_millis as u128;

        let mut store = self.store.lock().unwrap();
        store.insert(
            key,
            Value {
                data: value,
                expires_at: Some(expires_at),
            },
        );
    }

    fn get(&self, key: &str) -> Option<String> {
        let store = self.store.lock().unwrap();
        let value = store.get(key);
        if value.is_none() {
            return None;
        }

        let value = value.unwrap().clone();
        if value.expires_at.is_none() {
            return Some(value.data);
        }

        let now = current_timestamp();
        return if now < value.expires_at.unwrap() {
            Some(value.data)
        } else {
            None
        };
    }
}

fn current_timestamp() -> u128 {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
    return since_epoch.as_millis();
}
