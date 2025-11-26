use std::fmt;

/// Represents an unordered token pair keyed by mint addresses.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TokenPair {
    pub mint_a: String,
    pub mint_b: String,
}

impl TokenPair {
    pub fn new(left: &str, right: &str) -> Self {
        if left <= right {
            Self {
                mint_a: left.to_string(),
                mint_b: right.to_string(),
            }
        } else {
            Self {
                mint_a: right.to_string(),
                mint_b: left.to_string(),
            }
        }
    }

    pub fn from_array(raw: &[String; 2]) -> Self {
        Self::new(&raw[0], &raw[1])
    }

    pub fn matches(&self, left: &str, right: &str) -> bool {
        let other = TokenPair::new(left, right);
        self == &other
    }
}

impl fmt::Display for TokenPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.mint_a, self.mint_b)
    }
}

#[cfg(test)]
mod tests {
    use super::TokenPair;

    #[test]
    fn normalizes_ordering() {
        let unordered = TokenPair::new("b", "a");
        assert_eq!(unordered.mint_a, "a");
        assert_eq!(unordered.mint_b, "b");
    }

    #[test]
    fn matches_is_order_agnostic() {
        let pair = TokenPair::new("mint-x", "mint-y");
        assert!(pair.matches("mint-y", "mint-x"));
    }
}
