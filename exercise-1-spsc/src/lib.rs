// This is a simple implementation of a single producer single consumer channel
// using a fixed size ring buffer. The access to the buffer is synchronized using
// atomic operations, no mutexes are used. The buffer is implemented using an array
// of UnsafeCells with an Atomic Reference Counter, which allows us to both read and
//  write to the buffer from separate threads.
//
// The channel is implemented using 2 structs, Producer and Consumer, which both
// have a reference to the same buffer. The Producer writes to the buffer and the
// Consumer consumes elements from the buffer, replacing them with None values.
//
// The Producer will raise as SendError if the Consumer is not active anymore, e.g.
// if the Consumer was dropped. The Consumer will raise a RecvError if the Producer
// is not active anymore and all buffered elements have been read.
//
// We use 2 atomic indiced to keep track of the read and write index on the buffer.
// We make sure to synchronize the access to the buffer in the following way:
// - The Producer can write to the buffer, as long as its write index is ahead of the
//   read index of the Consumer.
// - Since we have a ring buffer, the Producer must ensure that it does not
//  overtake the Consumer when wrapping around the buffer, so the maximum write index
//  is the read index + the buffer size.
// - The Consumer can read from the buffer, as long as its read index is behind the
//   write index of the Producer.
// - The Consumer will continue to read from the buffer, even if the Producer is not
//   active anymore, as long as there are still elements in the buffer.

#![allow(unused_variables)]

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// checked and confirmed tests also ran with a small buffer size, e.g 32
const BUFFER_SIZE: usize = 1024;

pub struct Producer<T: Send> {
    message_buffer: Arc<[UnsafeCell<Option<T>>; BUFFER_SIZE]>,
    read_index: Arc<AtomicUsize>,
    write_index: Arc<AtomicUsize>,
    producer_counter: Arc<AtomicUsize>,
    consumer_counter: Arc<AtomicUsize>,
    _marker: PhantomData<T>,
}
pub struct Consumer<T: Send> {
    message_buffer: Arc<[UnsafeCell<Option<T>>; BUFFER_SIZE]>,
    read_index: Arc<AtomicUsize>,
    write_index: Arc<AtomicUsize>,
    producer_counter: Arc<AtomicUsize>,
    consumer_counter: Arc<AtomicUsize>,
    _marker: PhantomData<T>,
}

pub struct SPSC<T: Send> {
    producer: Producer<T>,
    consumer: Consumer<T>,
}

#[derive(Debug)]
pub struct SendError<T>(pub T);

#[derive(Debug)]
pub struct RecvError;

impl<T: Send> SPSC<T> {
    const INIT: UnsafeCell<Option<T>> = UnsafeCell::new(None);

    pub fn new() -> Self {
        // The only way I found for 2 threads to share a buffer is unsafe cells,
        // but I'm sure there is a better way and I  would love to know how to do it
        // in a more idiomatic / safe way!
        let cell_array: [UnsafeCell<Option<T>>; BUFFER_SIZE] = [Self::INIT; BUFFER_SIZE];
        let message_buffer: Arc<[UnsafeCell<Option<T>>; BUFFER_SIZE]> = Arc::new(cell_array);

        let read_index: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let write_index: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let producer_counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(1));
        let consumer_counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(1));

        let producer = Producer {
            message_buffer: message_buffer.clone(),
            read_index: read_index.clone(),
            write_index: write_index.clone(),
            producer_counter: producer_counter.clone(),
            consumer_counter: consumer_counter.clone(),
            _marker: PhantomData,
        };

        let consumer = Consumer {
            message_buffer: message_buffer.clone(),
            read_index: read_index.clone(),
            write_index: write_index.clone(),
            producer_counter: producer_counter.clone(),
            consumer_counter: consumer_counter.clone(),
            _marker: PhantomData,
        };

        SPSC { producer, consumer }
    }
}

impl<T: Send> Producer<T> {
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        loop {
            // If the consumer is not active anymore, we can not send any more messages
            if self.consumer_counter.load(Ordering::SeqCst) == 0 {
                return Err(SendError(val));
            }

            let write_index: usize = self.write_index.load(Ordering::SeqCst);

            // the producer can write to all places in the buffer, as long as it is
            // ahead of the consumer. However since we have a ring buffer, the producer
            // must ensure that it does not overtake the consumer when wrapping around
            // the buffer!
            if write_index < self.read_index.load(Ordering::SeqCst) + BUFFER_SIZE {
                unsafe {
                    self.message_buffer[write_index % BUFFER_SIZE]
                        .get()
                        .write(Some(val));
                }
                self.write_index.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
        }
    }
}

impl<T: Send> Consumer<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            let read_index: usize = self.read_index.load(Ordering::SeqCst);

            // When no producer is active and the consumer read all messages, we are done
            if self.producer_counter.load(Ordering::SeqCst) == 0 {
                if read_index == self.write_index.load(Ordering::SeqCst) {
                    return Err(RecvError);
                }
            }

            // since there is only one consumer, we do not need an atomic swap
            // to synchronize the read_index
            //
            // If the write_index changes during the load, it is okay because
            // the write index will always be greater than the read index and the
            // producer ensures, that the write index never overtakes the read index
            // when wrapping around the buffer
            if read_index < self.write_index.load(Ordering::SeqCst) {
                unsafe {
                    let val = self.message_buffer[read_index % BUFFER_SIZE]
                        .get()
                        .replace(None);

                    self.read_index.fetch_add(1, Ordering::SeqCst);
                    return Ok(val.unwrap());
                }
            }
        }
    }
}

impl<T: Send> Iterator for Consumer<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match self.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<T: Send> Iterator for &Consumer<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match self.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T: Send> Drop for Producer<T> {
    fn drop(&mut self) {
        self.producer_counter.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: Send> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.consumer_counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn channel<T: Send>() -> (Producer<T>, Consumer<T>) {
    let spsc: SPSC<T> = SPSC::new();
    return (spsc.producer, spsc.consumer);
}

// vorimplementierte Testsuite; bei Bedarf erweitern!

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::thread;

    use super::*;

    lazy_static! {
        static ref FOO_SET: Mutex<HashSet<i32>> = Mutex::new(HashSet::new());
    }

    #[derive(Debug)]
    struct Foo(i32);

    impl Foo {
        fn new(key: i32) -> Self {
            assert!(
                FOO_SET.lock().unwrap().insert(key),
                "double initialisation of element {}",
                key
            );
            Foo(key)
        }
    }

    impl Drop for Foo {
        fn drop(&mut self) {
            assert!(
                FOO_SET.lock().unwrap().remove(&self.0),
                "double free of element {}",
                self.0
            );
        }
    }

    // range of elements to be moved across the channel during testing
    const ELEMS: std::ops::Range<i32> = 0..1000;

    #[test]
    fn unused_elements_are_dropped() {
        lazy_static::initialize(&FOO_SET);

        for i in 0..100 {
            println!("Thread {} ", i);
            let (px, cx) = channel();
            let handle = thread::spawn(move || {
                for i in 0.. {
                    if px.send(Foo::new(i)).is_err() {
                        println!("AHHHHH on i: {}", i);
                        return;
                    }
                }
            });

            for _ in 0..i {
                cx.recv().unwrap();
            }

            drop(cx);

            assert!(handle.join().is_ok());

            let map = FOO_SET.lock().unwrap();
            if !map.is_empty() {
                panic!("FOO_MAP not empty: {:?}", *map);
            }
        }
    }

    #[test]
    fn elements_arrive_ordered() {
        let (px, cx) = channel();

        thread::spawn(move || {
            for i in ELEMS {
                px.send(i).unwrap();
            }
        });

        for i in ELEMS {
            assert_eq!(i, cx.recv().unwrap());
        }

        assert!(cx.recv().is_err());
    }

    #[test]
    fn all_elements_arrive() {
        for _ in 0..100 {
            let (px, cx) = channel();
            let handle = thread::spawn(move || {
                let mut count = 0;

                while let Ok(_) = cx.recv() {
                    count += 1;
                }

                count
            });

            thread::spawn(move || {
                for i in ELEMS {
                    px.send(i).unwrap();
                }
            });

            match handle.join() {
                Ok(count) => assert_eq!(count, ELEMS.len()),
                Err(_) => panic!("Error: join() returned Err"),
            }
        }
    }
}
