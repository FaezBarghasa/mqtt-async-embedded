//! # MQTT Topic Router and Trie Matcher
//!
//! Provides a high-performance topic router supporting exact topics,
//! single-level wildcards (`+`), and multi-level wildcards (`#`) according to the MQTT specification.

use std::boxed::Box;
use std::collections::HashMap;
use std::string::{String, ToString};
use std::vec::Vec;

use tokio::sync::mpsc;

use crate::tokio_client::types::{ClientError, PublishMessage};

/// Validates whether a topic filter string is compliant with the MQTT specification.
pub fn validate_topic_filter(filter: &str) -> Result<(), ClientError> {
    if filter.is_empty() {
        return Err(ClientError::InvalidTopic(
            "Topic filter cannot be empty".into(),
        ));
    }
    if filter.contains('\0') {
        return Err(ClientError::InvalidTopic(
            "Topic filter cannot contain null characters".into(),
        ));
    }

    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if *level == "#" {
            if i != levels.len() - 1 {
                return Err(ClientError::InvalidTopic(
                    "Multi-level wildcard '#' must be the last token in topic filter".into(),
                ));
            }
        } else if level.contains('#') {
            return Err(ClientError::InvalidTopic(
                "Invalid multi-level wildcard placement".into(),
            ));
        } else if level.contains('+') && *level != "+" {
            return Err(ClientError::InvalidTopic(
                "Single-level wildcard '+' must occupy an entire level".into(),
            ));
        }
    }
    Ok(())
}

/// Validates whether a publish topic string is compliant with the MQTT specification (cannot contain `+` or `#`).
pub fn validate_publish_topic(topic: &str) -> Result<(), ClientError> {
    if topic.is_empty() {
        return Err(ClientError::InvalidTopic(
            "Publish topic cannot be empty".into(),
        ));
    }
    if topic.contains('\0') {
        return Err(ClientError::InvalidTopic(
            "Publish topic cannot contain null characters".into(),
        ));
    }
    if topic.contains('+') || topic.contains('#') {
        return Err(ClientError::InvalidTopic(
            "Publish topic cannot contain wildcards ('+' or '#')".into(),
        ));
    }
    Ok(())
}

/// A trie node storing topic subscriptions.
#[derive(Default)]
struct TrieNode {
    exact_children: HashMap<String, TrieNode>,
    plus_child: Option<Box<TrieNode>>,
    hash_subscribers: Vec<mpsc::Sender<PublishMessage>>,
    exact_subscribers: Vec<mpsc::Sender<PublishMessage>>,
}

impl TrieNode {
    fn is_empty(&self) -> bool {
        self.exact_children.is_empty()
            && self.plus_child.is_none()
            && self.hash_subscribers.is_empty()
            && self.exact_subscribers.is_empty()
    }
}

/// A high-performance trie-based MQTT topic router.
pub struct TopicRouter {
    root: TrieNode,
}

impl Default for TopicRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicRouter {
    /// Creates a new empty topic router.
    pub fn new() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }

    /// Registers a subscription channel for the given topic filter.
    pub fn insert(
        &mut self,
        filter: &str,
        sender: mpsc::Sender<PublishMessage>,
    ) -> Result<(), ClientError> {
        validate_topic_filter(filter)?;
        let levels: Vec<&str> = filter.split('/').collect();
        let mut curr = &mut self.root;

        for (i, level) in levels.iter().enumerate() {
            if *level == "#" {
                curr.hash_subscribers.push(sender);
                return Ok(());
            } else if *level == "+" {
                curr = curr.plus_child.get_or_insert_with(Box::default);
            } else {
                curr = curr.exact_children.entry((*level).to_string()).or_default();
            }

            if i == levels.len() - 1 {
                curr.exact_subscribers.push(sender);
                return Ok(());
            }
        }
        Ok(())
    }

    /// Removes closed channels or unsubscribes a filter.
    pub fn remove(&mut self, filter: &str) -> Result<(), ClientError> {
        validate_topic_filter(filter)?;
        let levels: Vec<&str> = filter.split('/').collect();
        Self::remove_recursive(&mut self.root, &levels, 0);
        Ok(())
    }

    fn remove_recursive(node: &mut TrieNode, levels: &[&str], depth: usize) -> bool {
        if depth == levels.len() {
            node.exact_subscribers.clear();
            return node.is_empty();
        }

        let level = levels[depth];
        if level == "#" {
            node.hash_subscribers.clear();
            return node.is_empty();
        } else if level == "+" {
            if let Some(ref mut plus) = node.plus_child {
                if Self::remove_recursive(plus, levels, depth + 1) {
                    node.plus_child = None;
                }
            }
        } else if let Some(child) = node.exact_children.get_mut(level) {
            if Self::remove_recursive(child, levels, depth + 1) {
                node.exact_children.remove(level);
            }
        }

        node.is_empty()
    }

    /// Matches a published message's topic and dispatches cloned handles to all subscribed channels.
    ///
    /// Cleans up any dead/closed channel subscribers automatically.
    pub fn dispatch(&mut self, message: &PublishMessage) {
        let levels: Vec<&str> = message.topic.split('/').collect();
        Self::match_and_dispatch(&mut self.root, &levels, 0, message);
    }

    fn match_and_dispatch(
        node: &mut TrieNode,
        levels: &[&str],
        depth: usize,
        message: &PublishMessage,
    ) {
        // Dispatch to any multi-level wildcard (#) subscriptions on this node
        node.hash_subscribers.retain(|sender| {
            if sender.is_closed() {
                false
            } else {
                let _ = sender.try_send(message.clone());
                true
            }
        });

        if depth == levels.len() {
            // Reached the exact end of topic levels
            node.exact_subscribers.retain(|sender| {
                if sender.is_closed() {
                    false
                } else {
                    let _ = sender.try_send(message.clone());
                    true
                }
            });
            return;
        }

        let level = levels[depth];

        // 1. Check exact match
        if let Some(child) = node.exact_children.get_mut(level) {
            Self::match_and_dispatch(child, levels, depth + 1, message);
        }

        // 2. Check single-level wildcard (+)
        if let Some(ref mut plus) = node.plus_child {
            Self::match_and_dispatch(plus, levels, depth + 1, message);
        }
    }
}
