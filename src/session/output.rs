//! Bounded PTY output buffering and streaming UTF-8 decoding.

use super::{OutputChunk, OutputResponse, SessionStatus};
use chrono::Utc;
use std::collections::VecDeque;

pub(super) struct OutputBuffer {
    chunks: VecDeque<OutputChunk>,
    bytes: usize,
    max_bytes: usize,
    next_seq: u64,
    truncated_seq: Option<u64>,
}

impl OutputBuffer {
    pub(super) fn new(max_bytes: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            bytes: 0,
            max_bytes,
            next_seq: 0,
            truncated_seq: None,
        }
    }

    pub(super) fn push(&mut self, mut text: String) -> (u64, usize, usize) {
        let mut evicted_chunks = 0;
        let chunk_truncated = text.len() > self.max_bytes;
        if chunk_truncated {
            let mut start = text.len() - self.max_bytes;
            while !text.is_char_boundary(start) {
                start += 1;
            }
            text = text[start..].to_string();
            evicted_chunks = 1;
        }
        let len = text.len();
        let seq = self.next_seq;
        if chunk_truncated {
            self.truncated_seq = Some(seq);
        }
        self.next_seq += 1;
        self.bytes += len;
        self.chunks.push_back(OutputChunk {
            seq,
            timestamp: Utc::now(),
            text,
        });

        // The byte limit alone does not bound metadata for one-byte reads.
        while self.bytes > self.max_bytes || self.chunks.len() > 4096 {
            let Some(front) = self.chunks.pop_front() else {
                self.bytes = 0;
                break;
            };
            self.bytes = self.bytes.saturating_sub(front.text.len());
            evicted_chunks += 1;
        }
        (seq, self.bytes, evicted_chunks)
    }

    pub(super) fn since(&self, cursor: u64, status: SessionStatus) -> OutputResponse {
        let first_seq = self
            .chunks
            .front()
            .map(|chunk| chunk.seq)
            .unwrap_or(self.next_seq);
        let overrun = cursor < first_seq
            || self
                .truncated_seq
                .is_some_and(|truncated_seq| cursor <= truncated_seq);
        let chunks = self
            .chunks
            .iter()
            .filter(|chunk| chunk.seq >= cursor)
            .cloned()
            .collect();
        OutputResponse {
            cursor: self.next_seq,
            chunks,
            status,
            overrun,
            output_truncated: false,
        }
    }
}

/// Incrementally decodes PTY bytes without replacing a valid UTF-8 character
/// merely because its bytes straddle two OS reads.
#[derive(Default)]
pub(super) struct Utf8StreamDecoder {
    pending: Vec<u8>,
}

impl Utf8StreamDecoder {
    pub(super) fn push(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut output = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(text) => {
                    output.push_str(text);
                    self.pending.clear();
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid > 0 {
                        output.push_str(std::str::from_utf8(&self.pending[..valid]).unwrap_or(""));
                        self.pending.drain(..valid);
                    }
                    match error.error_len() {
                        Some(invalid_len) => {
                            output.push('\u{fffd}');
                            self.pending.drain(..invalid_len);
                        }
                        None => break,
                    }
                }
            }
        }
        output
    }

    pub(super) fn finish(self) -> String {
        String::from_utf8_lossy(&self.pending).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_buffer_tracks_cursor_and_overrun() {
        let mut buffer = OutputBuffer::new(5);
        buffer.push("abc".to_string());
        buffer.push("def".to_string());
        let response = buffer.since(0, SessionStatus::Running);
        assert!(response.overrun);
        assert_eq!(response.cursor, 2);
        assert_eq!(response.chunks[0].text, "def");
    }

    #[test]
    fn oversized_output_retains_a_bounded_utf8_tail() {
        let mut buffer = OutputBuffer::new(5);
        buffer.push("开始abcdef".to_string());
        assert!(buffer.bytes <= 5);
        let response = buffer.since(0, SessionStatus::Running);
        assert!(response.overrun);
        assert_eq!(response.chunks[0].text, "bcdef");
    }

    #[test]
    fn utf8_decoder_preserves_characters_split_across_reads() {
        let mut decoder = Utf8StreamDecoder::default();
        let bytes = "完成".as_bytes();
        assert_eq!(decoder.push(&bytes[..2]), "");
        assert_eq!(decoder.push(&bytes[2..4]), "完");
        assert_eq!(decoder.push(&bytes[4..]), "成");
        assert_eq!(decoder.finish(), "");
    }

    #[test]
    fn small_reads_also_have_a_metadata_bound() {
        let mut buffer = OutputBuffer::new(1024 * 1024);
        for _ in 0..5000 {
            buffer.push("x".into());
        }
        let output = buffer.since(0, SessionStatus::Running);
        assert_eq!(output.chunks.len(), 4096);
        assert!(output.overrun);
        assert_eq!(output.cursor, 5000);
    }

    #[test]
    fn utf8_decoder_keeps_valid_data_after_invalid_and_incomplete_bytes() {
        let mut decoder = Utf8StreamDecoder::default();
        assert_eq!(decoder.push(&[b'a', 0xff, b'b', 0xe4]), "a\u{fffd}b");
        assert_eq!(decoder.finish(), "\u{fffd}");
    }
}
