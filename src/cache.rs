use std::{
    collections::{BinaryHeap, HashMap},
    hash::Hasher,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

const CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Eq, PartialEq)]
struct CacheItem {
    key: String,
    value: String,
    expiration_time: Option<std::time::Instant>,
}

impl PartialOrd for CacheItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CacheItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.expiration_time.is_none() {
            return std::cmp::Ordering::Less;
        }

        other.expiration_time.cmp(&self.expiration_time)
    }
}

#[derive(Debug)]
struct Shard {
    pq: Arc<Mutex<BinaryHeap<Arc<CacheItem>>>>,
    items: Arc<Mutex<HashMap<String, Arc<CacheItem>>>>,
}

impl Shard {
    fn new() -> Self {
        Self {
            pq: Arc::new(Mutex::new(BinaryHeap::new())),
            items: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn set(&mut self, key: &str, value: &str, ttl: Option<Duration>) {
        let item = Arc::new(CacheItem {
            key: key.to_string(),
            value: value.to_string(),
            expiration_time: ttl.map(|ttl| std::time::Instant::now() + ttl),
        });

        let mut items = self.items.lock().unwrap();

        // If the key didn't already exist add it to the queue.
        if items.get(key).is_none() {
            let mut pq = self.pq.lock().unwrap();
            pq.push(item.clone());
        }

        items.insert(key.to_string(), item.clone());
    }

    fn get(&self, key: &str) -> Option<String> {
        let items = self.items.lock().unwrap();
        items.get(key).and_then(|item| match item.expiration_time {
            Some(expiry) if expiry > std::time::Instant::now() => Some(item.value.to_string()),
            Some(_) => None,
            None => Some(item.value.to_string()),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Cache {
    shards: Vec<Arc<Mutex<Shard>>>,
    #[allow(dead_code)]
    txs: Vec<std::sync::mpsc::Sender<()>>,
}

impl Cache {
    pub(crate) fn new(number_of_shards: u64) -> Self {
        let mut shards = Vec::new();
        let mut txs: Vec<std::sync::mpsc::Sender<()>> = Vec::new();

        for _ in 0..number_of_shards {
            let (tx, rx) = std::sync::mpsc::channel();
            txs.push(tx);

            let shard = Arc::new(Mutex::new(Shard::new()));
            shards.push(shard.clone());

            thread::spawn(move || {
                while let Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                    rx.recv_timeout(CLEANUP_INTERVAL)
                {
                    tracing::debug!("Running eviction loop");

                    let shard = shard.lock().unwrap();
                    let mut pq = shard.pq.lock().unwrap();
                    let mut items = shard.items.lock().unwrap();
                    let now = std::time::Instant::now();

                    while let Some(item) = pq.peek() {
                        let expiry = if let Some(expiry) = item.expiration_time {
                            expiry
                        } else {
                            tracing::debug!("No items with TTL in queue!");
                            break;
                        };

                        if expiry > now {
                            tracing::debug!("No items with TTL that expired!");
                            break;
                        }

                        if let Some(item) = pq.pop() {
                            if let Some(item) = items.remove(&item.key) {
                                if let Some(expiry) = item.expiration_time {
                                    // Item in pq was expired but map was updated. Re-insert
                                    // and continue.
                                    if expiry > now {
                                        tracing::debug!(
                                            "Item has been updated - should not evict!"
                                        );

                                        pq.push(item.clone());
                                        items.insert(item.key.clone(), item);
                                    } else {
                                        tracing::debug!("Evicting item - it was expired!");
                                    }
                                }
                            }
                        }
                    }
                }

                tracing::debug!("Evicion loop terminated");
            });
        }

        Self { shards, txs }
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        let index = shard_from_key(key, self.shards.len() as u64) as usize;
        self.shards[index].lock().unwrap().get(key)
    }

    pub(crate) fn set(&mut self, key: &str, value: &str, ttl: Option<std::time::Duration>) {
        let index = shard_from_key(key, self.shards.len() as u64) as usize;
        self.shards[index].lock().unwrap().set(key, value, ttl)
    }
}

fn hash_for_key(key: &str) -> u64 {
    let mut hasher = fnv::FnvHasher::default();
    hasher.write(key.as_bytes());
    hasher.finish()
}
fn shard_from_key(key: &str, shards: u64) -> u64 {
    hash_for_key(key) % shards
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ordering() {
        let mut cache = Cache::new(3);
        cache.set("k", "v", Some(std::time::Duration::from_secs(1)));
        cache.set("k3", "v3", None);
        cache.set("k2", "v2", Some(std::time::Duration::from_secs(3)));

        println!(
            "{:?}, {:?}, {:?}",
            cache.get("k"),
            cache.get("k2"),
            cache.get("k3")
        );
        cache.set("k2", "v2", Some(std::time::Duration::from_secs(10)));

        std::thread::sleep(std::time::Duration::from_secs(5));
        println!(
            "{:?}, {:?}, {:?}",
            cache.get("k"),
            cache.get("k2"),
            cache.get("k3")
        );

        drop(cache);
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
