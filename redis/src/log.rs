use std::{fmt::Display, thread};

pub fn debug<T : Display>(msg: T) {
    println!("[DEBUG] @ {:?}: {}", thread::current().id(), msg);
}

pub fn info<T : Display>(msg: T) {
    println!("[INFO] @ {:?}: {}", thread::current().id(), msg);
}

pub fn error<T: Display>(msg: T) {
    println!("[ERROR] @ {:?}: {}", thread::current().id(), msg);
}

