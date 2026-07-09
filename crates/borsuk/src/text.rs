use std::collections::BTreeMap;

/// Tokenizer used to turn text into indexable terms.
pub trait Tokenizer: Send + Sync {
    /// Tokenize a text payload into normalized term strings.
    fn tokenize(&self, text: &str) -> Vec<String>;

    /// Return a short stable tokenizer identity string.
    fn fingerprint(&self) -> String;
}

/// Unicode word tokenizer that splits on non-alphanumeric boundaries and lowercases terms.
#[derive(Debug, Clone)]
pub struct UnicodeWordLowercase;

impl Tokenizer for UnicodeWordLowercase {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut token = String::new();

        for character in text.chars() {
            if character.is_alphanumeric() {
                token.extend(character.to_lowercase());
            } else if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
        }
        if !token.is_empty() {
            tokens.push(token);
        }

        tokens
    }

    fn fingerprint(&self) -> String {
        "unicode-word-lower@1".to_string()
    }
}

/// Tokenizer that splits text with [`str::split_whitespace`].
#[derive(Debug, Clone)]
pub struct Whitespace;

impl Tokenizer for Whitespace {
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split_whitespace().map(ToOwned::to_owned).collect()
    }

    fn fingerprint(&self) -> String {
        "whitespace@1".to_string()
    }
}

/// Tokenizer that emits contiguous lowercased character n-grams.
#[derive(Debug, Clone)]
pub struct CharNgram {
    /// Number of characters per n-gram.
    pub n: usize,
}

impl Tokenizer for CharNgram {
    fn tokenize(&self, text: &str) -> Vec<String> {
        if self.n == 0 {
            return Vec::new();
        }

        let characters = text
            .chars()
            .flat_map(char::to_lowercase)
            .collect::<Vec<_>>();
        characters
            .windows(self.n)
            .map(|window| window.iter().collect())
            .collect()
    }

    fn fingerprint(&self) -> String {
        format!("char-ngram-{}@1", self.n)
    }
}

/// Return a deterministic stable term id for a token using 32-bit FNV-1a.
#[must_use]
pub fn term_id(token: &str) -> u32 {
    const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
    const FNV_PRIME: u32 = 0x0100_0193;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in token.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Tokenize text and return term frequencies sorted by term id.
#[must_use]
pub fn term_frequencies(tokenizer: &dyn Tokenizer, text: &str) -> BTreeMap<u32, u32> {
    let mut frequencies = BTreeMap::new();
    for token in tokenizer.tokenize(text) {
        *frequencies.entry(term_id(&token)).or_insert(0) += 1;
    }
    frequencies
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_word_lowercase_counts_terms() {
        let terms = term_frequencies(&UnicodeWordLowercase, "Hello, WORLD! hello");

        assert_eq!(terms.get(&term_id("hello")), Some(&2));
        assert_eq!(terms.get(&term_id("world")), Some(&1));
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn term_ids_are_deterministic() {
        assert_eq!(term_id("hello"), term_id("hello"));
        assert_eq!(term_id("hello"), 0x4f9f_2cab);
        assert_ne!(term_id("hello"), term_id("world"));
    }

    #[test]
    fn whitespace_preserves_whitespace_delimited_terms() {
        let tokenizer = Whitespace;

        assert_eq!(
            tokenizer.tokenize("Hello  WORLD\nhello"),
            vec![
                "Hello".to_string(),
                "WORLD".to_string(),
                "hello".to_string(),
            ]
        );
        assert_eq!(tokenizer.fingerprint(), "whitespace@1");
    }

    #[test]
    fn char_ngram_emits_lowercased_character_windows() {
        let tokenizer = CharNgram { n: 2 };

        assert_eq!(
            tokenizer.tokenize("AbC"),
            vec!["ab".to_string(), "bc".to_string(),]
        );
        assert_eq!(tokenizer.fingerprint(), "char-ngram-2@1");
    }
}
