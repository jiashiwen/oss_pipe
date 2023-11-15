use anyhow::Result;
use regex::RegexSet;

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
        exclude_regex: Option<Vec<String>>,
        include_regex: Option<Vec<String>>,
    ) -> Result<Self> {
        let exclude_regex = match exclude_regex {
            Some(v) => Some(RegexSet::new(&v)?),
            None => None,
        };
        let include_regex = match include_regex {
            Some(v) => Some(RegexSet::new(&v)?),
            None => None,
        };

        Ok(Self {
            exclude_regex,
            include_regex,
        })
    }
    pub fn new(exclude_regex: Option<RegexSet>, include_regex: Option<RegexSet>) -> Self {
        Self {
            exclude_regex,
            include_regex,
        }
    }
    pub fn set_exclude(&mut self, reg_set: RegexSet) {
        self.exclude_regex = Some(reg_set);
    }

    pub fn set_include(&mut self, reg_set: RegexSet) {
        self.include_regex = Some(reg_set);
    }

    pub fn filter(self, content: &str) -> bool {
        match self.exclude_regex {
            Some(e) => {
                return !e.is_match(content);
            }
            None => {}
        }

        match self.include_regex {
            Some(i) => return i.is_match(content),
            None => {}
        }

        true
    }
}
