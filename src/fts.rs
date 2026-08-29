use std::collections::{HashMap, HashSet};
use regex::Regex;
use crate::schema::*;
use crate::error::SkvsError;
use std::sync::Arc;

pub struct FtsIndex {
    inverted: HashMap<String, HashSet<RowId>>,
    doc_lengths: HashMap<RowId, usize>,
    total_docs: usize,
}

impl FtsIndex {
    pub fn new() -> Self {
        FtsIndex {
            inverted: HashMap::new(),
            doc_lengths: HashMap::new(),
            total_docs: 0,
        }
    }

    pub fn add_document(&mut self, rowid: RowId, content: &str) {
        let tokens = self.tokenize(content);
        self.doc_lengths.insert(rowid, tokens.len());
        self.total_docs += 1;
        for token in tokens {
            self.inverted.entry(token).or_default().insert(rowid);
        }
    }

    pub fn remove_document(&mut self, rowid: RowId, content: &str) {
        let tokens = self.tokenize(content);
        for token in tokens {
            if let Some(docs) = self.inverted.get_mut(&token) {
                docs.remove(&rowid);
                if docs.is_empty() {
                    self.inverted.remove(&token);
                }
            }
        }
        self.doc_lengths.remove(&rowid);
        self.total_docs -= 1;
    }

    pub fn search(&self, query: &str) -> Vec<RowId> {
        let tokens = self.tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }
        let mut results: Option<HashSet<RowId>> = None;
        for token in tokens {
            if let Some(docs) = self.inverted.get(&token) {
                if let Some(current) = results {
                    results = Some(current.intersection(docs).cloned().collect());
                } else {
                    results = Some(docs.clone());
                }
            } else {
                return vec![];
            }
        }
        results.map(|set| set.into_iter().collect()).unwrap_or_default()
    }

    pub fn search_with_rank(&self, query: &str) -> Vec<(RowId, f64)> {
        let tokens = self.tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }
        let avg_doc_len = self.doc_lengths.values().sum::<usize>() as f64 / self.total_docs as f64;
        let mut scores: HashMap<RowId, f64> = HashMap::new();
        for token in tokens {
            if let Some(docs) = self.inverted.get(&token) {
                let doc_freq = docs.len() as f64;
                let idf = ((self.total_docs as f64 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
                for &doc_id in docs {
                    let doc_len = *self.doc_lengths.get(&doc_id).unwrap_or(&0) as f64;
                    let score = idf * ((doc_len / avg_doc_len + 0.5) / (doc_len / avg_doc_len + 1.0));
                    *scores.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }
        let mut results: Vec<(RowId, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        let re = Regex::new(r"[a-zA-Z0-9]+").unwrap();
        let stopwords: HashSet<&str> = ["a", "an", "the", "is", "are", "was", "were", "in", "on", "at", "to", "for", "of", "with", "without", "and", "or", "but", "so", "for", "yet"].iter().cloned().collect();
        re.find_iter(&text.to_lowercase())
            .map(|m| m.as_str().to_string())
            .filter(|s| s.len() > 2 && !stopwords.contains(s.as_str()))
            .collect()
    }
}

pub struct FtsVirtualTable {
    pub name: String,
    pub index: Arc<tokio::sync::Mutex<FtsIndex>>,
    pub content_column: String,
}

impl FtsVirtualTable {
    pub fn new(name: &str, content_column: &str) -> Self {
        FtsVirtualTable {
            name: name.to_string(),
            index: Arc::new(tokio::sync::Mutex::new(FtsIndex::new())),
            content_column: content_column.to_string(),
        }
    }

    pub async fn insert(&self, rowid: RowId, content: &str) -> Result<(), SkvsError> {
        let mut index = self.index.lock().await;
        index.add_document(rowid, content);
        Ok(())
    }

    pub async fn delete(&self, rowid: RowId, content: &str) -> Result<(), SkvsError> {
        let mut index = self.index.lock().await;
        index.remove_document(rowid, content);
        Ok(())
    }

    pub async fn search(&self, query: &str) -> Vec<RowId> {
        let index = self.index.lock().await;
        index.search(query)
    }

    pub async fn search_with_rank(&self, query: &str) -> Vec<(RowId, f64)> {
        let index = self.index.lock().await;
        index.search_with_rank(query)
    }
}