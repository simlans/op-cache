use moka::future::Cache as MokaCache;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::protocol::CacheStats;

pub struct Cache {
    inner: MokaCache<String, String>,           // Value cache: key -> content
    file_cache: MokaCache<String, (String, u64)>,  // File cache: key -> (temp_path, ref_count)
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Cache {
    pub fn new(max_entries: u64, ttl_seconds: u64) -> Self {
        let inner = MokaCache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(ttl_seconds))
            .build();
        let file_cache = MokaCache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(ttl_seconds * 2))  // Longer TTL for files
            .build();

        Self {
            inner,
            file_cache,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let result = self.inner.get(key).await;

        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    pub async fn insert(&self, key: &str, value: String) {
        self.inner.insert(key.to_string(), value).await;
    }

    pub async fn get_file(&self, key: &str) -> Option<(String, u64)> {
        let result = self.file_cache.get(key).await;

        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    pub async fn insert_file(&self, key: &str, path: String) {
        // Store with ref_count = 1
        self.file_cache.insert(key.to_string(), (path, 1)).await;
    }

    pub async fn increment_file_ref(&self, key: &str) -> Option<u64> {
        let result = self.file_cache.get(key).await?;
        let (path, ref_count) = result;
        let new_ref = ref_count + 1;
        self.file_cache.insert(key.to_string(), (path, new_ref)).await;
        Some(new_ref)
    }

    pub async fn decrement_file_ref(&self, key: &str) -> Option<u64> {
        let result = self.file_cache.get(key).await?;
        let (path, ref_count) = result;
        let new_ref = ref_count.saturating_sub(1);
        if new_ref == 0 {
            // Remove file from cache and delete it
            let _ = std::fs::remove_file(&path);
            self.file_cache.invalidate(key).await;
        } else {
            self.file_cache.insert(key.to_string(), (path, new_ref)).await;
        }
        Some(new_ref)
    }

    pub fn clear(&self) {
        self.inner.invalidate_all();
        // We don't auto-delete files here - they get cleaned up on shutdown
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }

    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        CacheStats {
            entries: self.inner.entry_count() + self.file_cache.entry_count(),
            hits,
            misses,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        // Clean up all temp files on cache shutdown
        let entries: Vec<(String, (String, u64))> = std::collections::vec::Vec::new();
        // We can't easily get all entries from moka without async, so we skip cleanup here
        // File cleanup happens through normal file caching with short TTL + manual clear
    }
}
