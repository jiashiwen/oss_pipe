use anyhow::Result;
use regex::RegexSet;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct RegexFilter {
    pub exclude_regex: Option<RegexSet>,
    pub include_regex: Option<RegexSet>,
}

impl Default for RegexFilter {
    fn default() -> Self {
        Self {
            exclude_regex: None,
            include_regex: None,
        }
    }
}

impl RegexFilter {
    pub fn from_vec(
        exclude_regex: &Option<Vec<String>>,
        include_regex: &Option<Vec<String>>,
    ) -> Result<Self> {
        let exclude_regex = match exclude_regex {
            Some(v) => Some(RegexSet::new(v)?),
            None => None,
        };
        let include_regex = match include_regex {
            Some(v) => Some(RegexSet::new(v)?),
            None => None,
        };

        Ok(Self {
            exclude_regex,
            include_regex,
        })
    }

    #[allow(dead_code)]
    pub fn new(exclude_regex: Option<RegexSet>, include_regex: Option<RegexSet>) -> Self {
        Self {
            exclude_regex,
            include_regex,
        }
    }

    #[allow(dead_code)]
    pub fn set_exclude(&mut self, reg_set: RegexSet) {
        self.exclude_regex = Some(reg_set);
    }

    #[allow(dead_code)]
    pub fn set_include(&mut self, reg_set: RegexSet) {
        self.include_regex = Some(reg_set);
    }

    pub fn filter(&self, content: &str) -> bool {
        match self.exclude_regex.clone() {
            Some(e) => {
                return !e.is_match(content);
            }
            None => {}
        }

        match self.include_regex.clone() {
            Some(i) => return i.is_match(content),
            None => {}
        }

        true
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum LastModifyFilterType {
    Greater,
    Less,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct LastModifyFilter {
    pub filter_type: LastModifyFilterType,
    pub timestamp: usize,
}

impl LastModifyFilter {
    pub fn filter(&self, timestamp: usize) -> bool {
        match self.filter_type {
            LastModifyFilterType::Greater => timestamp.ge(&self.timestamp),
            LastModifyFilterType::Less => timestamp.le(&self.timestamp),
        }
    }
}
