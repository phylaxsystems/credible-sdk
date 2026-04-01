//! Terraform-style diff display for deployment plan changes.
//!
//! Deserializes the preview endpoint's response and renders a colored plan
//! output. Diff types are reused from the generated API client; only
//! [`PreviewResponse`] is defined here (the preview endpoint is not yet in
//! the generated client).

use colored::Colorize;
use dapp_api_client::generated::client::types::{
    PostProjectsProjectIdReleasesResponseDiff as ReleaseDiff,
    PostProjectsProjectIdReleasesResponseDiffContractsValue as ContractDiffEntry,
    PostProjectsProjectIdReleasesResponseDiffContractsValueAssertionsItem as AssertionDiffEntry,
    PostProjectsProjectIdReleasesResponseDiffContractsValueAssertionsItemChangeType as AssertionChangeType,
    PostProjectsProjectIdReleasesResponseDiffContractsValueChangeType as ContractChangeType,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::fmt::Write;
use uuid::Uuid;

/// Message displayed when the diff contains no changes.
pub const NO_CHANGES_MESSAGE: &str = "No changes. Project is up-to-date.";

/// Top-level response from `POST /projects/{id}/releases/preview`.
///
/// The preview endpoint is not yet part of the generated API client, so this
/// wrapper is defined manually. The inner [`ReleaseDiff`] reuses the generated
/// type shared with the release-creation response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviewResponse {
    #[serde(rename = "hasChanges")]
    pub has_changes: bool,
    #[serde(rename = "configMismatch")]
    pub config_mismatch: bool,
    #[serde(rename = "driftDetected")]
    pub drift_detected: bool,
    pub diff: ReleaseDiff,
    #[serde(rename = "diffedAgainstReleaseId")]
    pub diffed_against_release_id: Option<Uuid>,
}

impl PreviewResponse {
    /// Returns `true` if the preview contains any changes to apply.
    pub fn has_changes(&self) -> bool {
        self.has_changes
    }

    /// Renders the deployment plan as a Terraform-style colored string.
    pub fn render_plan(&self) -> String {
        render_plan(&self.diff)
    }
}

/// Renders the deployment plan as a Terraform-style colored string.
fn render_plan(diff: &ReleaseDiff) -> String {
    let mut out = String::new();
    writeln!(out, "pcl apply \u{2014} Deployment Plan").unwrap();
    writeln!(out).unwrap();

    // Sort contracts by label for deterministic output, grouped by change type.
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut modified = Vec::new();
    let mut unchanged = Vec::new();

    for (label, entry) in &diff.contracts {
        match entry.change_type {
            ContractChangeType::Added => added.push((label.as_str(), entry)),
            ContractChangeType::Removed => removed.push((label.as_str(), entry)),
            ContractChangeType::Modified => modified.push((label.as_str(), entry)),
            ContractChangeType::Unchanged => unchanged.push((label.as_str(), entry)),
        }
    }

    added.sort_by_key(|(label, _)| *label);
    removed.sort_by_key(|(label, _)| *label);
    modified.sort_by_key(|(label, _)| *label);
    unchanged.sort_by_key(|(label, _)| *label);

    for (label, entry) in &added {
        render_added(&mut out, label, entry);
    }
    for (label, entry) in &removed {
        render_removed(&mut out, label, entry);
    }
    for (label, entry) in &modified {
        render_modified(&mut out, label, entry);
    }
    for (label, _) in &unchanged {
        render_unchanged(&mut out, label);
    }

    let s = &diff.summary.assertions;
    writeln!(
        out,
        "Plan: {} added, {} changed, {} removed.",
        s.added, s.modified, s.removed,
    )
    .unwrap();

    out
}

/// Abbreviates a hex address to `0xABCD...WXYZ` when longer than 10 characters.
fn abbreviate_address(addr: &str) -> String {
    if addr.len() > 10 && addr.starts_with("0x") {
        format!("{}...{}", &addr[..6], &addr[addr.len() - 4..])
    } else {
        addr.to_string()
    }
}

fn render_added(out: &mut String, label: &str, entry: &ContractDiffEntry) {
    let addr = abbreviate_address(&entry.address);
    writeln!(out, "  {} contract \"{label}\" ({addr})", "+".green()).unwrap();
    for a in &entry.assertions {
        if let Some(file) = &a.file {
            writeln!(out, "      {} {file}", "+".green()).unwrap();
        }
    }
    writeln!(out).unwrap();
}

fn render_removed(out: &mut String, label: &str, entry: &ContractDiffEntry) {
    let addr = abbreviate_address(&entry.address);
    writeln!(out, "  {} contract \"{label}\" ({addr})", "-".red()).unwrap();
    for a in &entry.assertions {
        if let Some(file) = &a.file {
            writeln!(out, "      {} {file}", "-".red()).unwrap();
        }
    }
    writeln!(out).unwrap();
}

fn render_modified(out: &mut String, label: &str, entry: &ContractDiffEntry) {
    let addr = abbreviate_address(&entry.address);
    writeln!(out, "  {} contract \"{label}\" ({addr})", "~".yellow()).unwrap();

    if let Some(meta) = &entry.metadata_changes {
        if let Some(name_change) = &meta.name {
            let from = name_change.from.as_deref().unwrap_or("(none)");
            let to = name_change.to.as_deref().unwrap_or("(none)");
            writeln!(out, "      name: \"{from}\" \u{2192} \"{to}\"").unwrap();
        }
        if let Some(addr_change) = &meta.address {
            writeln!(
                out,
                "      address: {} \u{2192} {}",
                abbreviate_address(&addr_change.from),
                abbreviate_address(&addr_change.to),
            )
            .unwrap();
        }
    }

    for a in &entry.assertions {
        render_assertion_diff(out, a);
    }
    writeln!(out).unwrap();
}

fn render_assertion_diff(out: &mut String, a: &AssertionDiffEntry) {
    let file = a.file.as_deref().unwrap_or("(unknown)");
    match a.change_type {
        AssertionChangeType::Added => {
            writeln!(out, "      {} {file}", "+".green()).unwrap();
        }
        AssertionChangeType::Removed => {
            writeln!(out, "      {} {file}", "-".red()).unwrap();
        }
        AssertionChangeType::Modified => {
            writeln!(out, "      {} {file}", "~".yellow()).unwrap();
            if let Some(vc) = &a.compiler_version_change {
                writeln!(out, "          compiler: {} \u{2192} {}", vc.from, vc.to).unwrap();
            }
        }
        AssertionChangeType::Unchanged => {}
    }
}

fn render_unchanged(out: &mut String, label: &str) {
    writeln!(
        out,
        "  {}",
        format!("contract \"{label}\" (unchanged)").dimmed(),
    )
    .unwrap();
    writeln!(out).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn disable_colors() {
        colored::control::set_override(false);
    }

    fn sample_preview_json() -> serde_json::Value {
        json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "price_feed": {
                        "address": "0x1234567890abcdef1234567890abcdef12345678",
                        "name": "Token Price Feed",
                        "changeType": "added",
                        "metadataChanges": null,
                        "assertions": [{
                            "file": "PriceFeedAssertion.a.sol",
                            "args": [],
                            "changeType": "added",
                            "assertionId": "0xabc",
                            "previousAssertionId": null,
                            "compilerVersionChange": null
                        }]
                    },
                    "old_contract": {
                        "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                        "name": "Old Contract",
                        "changeType": "removed",
                        "metadataChanges": null,
                        "assertions": [{
                            "file": "OldAssertion.sol",
                            "args": [],
                            "changeType": "removed",
                            "assertionId": "0xdef",
                            "previousAssertionId": null,
                            "compilerVersionChange": null
                        }]
                    },
                    "simple_lending": {
                        "address": "0xDeaDbEeFDeaDbEeFDeaDbEeFDeaDbEeFDeaDbEeF",
                        "name": "Simple Lending Protocol",
                        "changeType": "modified",
                        "metadataChanges": {
                            "name": { "from": "Simple Lending", "to": "Simple Lending Protocol" }
                        },
                        "assertions": [
                            {
                                "file": "SimpleLendingAssertion.a.sol",
                                "args": [],
                                "changeType": "added",
                                "assertionId": "0x111",
                                "previousAssertionId": null,
                                "compilerVersionChange": null
                            },
                            {
                                "file": "OldAssertion.sol",
                                "args": [],
                                "changeType": "removed",
                                "assertionId": "0x222",
                                "previousAssertionId": null,
                                "compilerVersionChange": null
                            },
                            {
                                "file": "MockAssertion.sol",
                                "args": ["0x1234"],
                                "changeType": "modified",
                                "assertionId": "0x333",
                                "previousAssertionId": "0x444",
                                "compilerVersionChange": null
                            }
                        ]
                    },
                    "stable_contract": {
                        "address": "0x5555555555555555555555555555555555555555",
                        "name": "Stable",
                        "changeType": "unchanged",
                        "metadataChanges": null,
                        "assertions": [{
                            "file": "Stable.sol",
                            "args": [],
                            "changeType": "unchanged",
                            "assertionId": "0x555",
                            "previousAssertionId": null,
                            "compilerVersionChange": null
                        }]
                    }
                },
                "summary": {
                    "contracts": { "added": 1, "removed": 1, "modified": 1, "unchanged": 1 },
                    "assertions": { "added": 2, "removed": 2, "modified": 1, "unchanged": 1 }
                }
            },
            "diffedAgainstReleaseId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        })
    }

    #[test]
    fn deserializes_full_preview_response() {
        let preview: PreviewResponse = serde_json::from_value(sample_preview_json()).unwrap();

        assert!(preview.has_changes);
        assert!(!preview.config_mismatch);
        assert!(!preview.drift_detected);
        assert!(preview.diffed_against_release_id.is_some());

        assert_eq!(preview.diff.contracts.len(), 4);

        let added = &preview.diff.contracts["price_feed"];
        assert_eq!(added.change_type, ContractChangeType::Added);
        assert_eq!(added.assertions.len(), 1);
        assert_eq!(
            added.assertions[0].file.as_deref(),
            Some("PriceFeedAssertion.a.sol")
        );

        let removed = &preview.diff.contracts["old_contract"];
        assert_eq!(removed.change_type, ContractChangeType::Removed);

        let modified = &preview.diff.contracts["simple_lending"];
        assert_eq!(modified.change_type, ContractChangeType::Modified);
        assert!(modified.metadata_changes.is_some());
        let meta = modified.metadata_changes.as_ref().unwrap();
        assert_eq!(
            meta.name.as_ref().unwrap().from.as_deref(),
            Some("Simple Lending")
        );
        assert_eq!(
            meta.name.as_ref().unwrap().to.as_deref(),
            Some("Simple Lending Protocol")
        );
        assert_eq!(modified.assertions.len(), 3);

        let unchanged = &preview.diff.contracts["stable_contract"];
        assert_eq!(unchanged.change_type, ContractChangeType::Unchanged);

        assert_eq!(preview.diff.summary.contracts.added, 1);
        assert_eq!(preview.diff.summary.contracts.removed, 1);
        assert_eq!(preview.diff.summary.contracts.modified, 1);
        assert_eq!(preview.diff.summary.contracts.unchanged, 1);
    }

    #[test]
    fn deserializes_minimal_preview() {
        let json = json!({
            "hasChanges": false,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {},
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 },
                    "assertions": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        assert!(!preview.has_changes());
        assert!(preview.diff.contracts.is_empty());
        assert!(preview.diffed_against_release_id.is_none());
    }

    #[test]
    fn has_changes_returns_api_field() {
        let mut preview: PreviewResponse = serde_json::from_value(sample_preview_json()).unwrap();
        assert!(preview.has_changes());

        preview.has_changes = false;
        assert!(!preview.has_changes());
    }

    #[test]
    fn renders_full_plan() {
        disable_colors();

        let preview: PreviewResponse = serde_json::from_value(sample_preview_json()).unwrap();
        let output = preview.render_plan();

        assert!(output.starts_with("pcl apply \u{2014} Deployment Plan\n"));

        // Added
        assert!(output.contains("+ contract \"price_feed\" (0x1234...5678)"));
        assert!(output.contains("+ PriceFeedAssertion.a.sol"));

        // Removed
        assert!(output.contains("- contract \"old_contract\" (0xdead...beef)"));
        assert!(output.contains("- OldAssertion.sol"));

        // Modified
        assert!(output.contains("~ contract \"simple_lending\" (0xDeaD...bEeF)"));
        assert!(output.contains("name: \"Simple Lending\" \u{2192} \"Simple Lending Protocol\""));
        assert!(output.contains("+ SimpleLendingAssertion.a.sol"));
        assert!(output.contains("- OldAssertion.sol"));
        assert!(output.contains("~ MockAssertion.sol"));

        // Unchanged
        assert!(output.contains("contract \"stable_contract\" (unchanged)"));

        // Summary uses assertion-level totals
        assert!(output.contains("Plan: 2 added, 1 changed, 2 removed."));
    }

    #[test]
    fn renders_added_only() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "new_contract": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "name": "New",
                        "changeType": "added",
                        "metadataChanges": null,
                        "assertions": [{
                            "file": "New.sol", "args": [], "changeType": "added",
                            "assertionId": null, "previousAssertionId": null,
                            "compilerVersionChange": null
                        }]
                    }
                },
                "summary": {
                    "contracts": { "added": 1, "removed": 0, "modified": 0, "unchanged": 0 },
                    "assertions": { "added": 1, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("+ contract \"new_contract\" (0xaaaa...aaaa)"));
        assert!(output.contains("Plan: 1 added, 0 changed, 0 removed."));
    }

    #[test]
    fn renders_removed_only() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "gone": {
                        "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                        "name": "Gone",
                        "changeType": "removed",
                        "metadataChanges": null,
                        "assertions": [
                            { "file": "X.sol", "args": [], "changeType": "removed", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null },
                            { "file": "Y.sol", "args": [], "changeType": "removed", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null }
                        ]
                    }
                },
                "summary": {
                    "contracts": { "added": 0, "removed": 1, "modified": 0, "unchanged": 0 },
                    "assertions": { "added": 0, "removed": 2, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("- contract \"gone\" (0xdead...beef)"));
        assert!(output.contains("- X.sol"));
        assert!(output.contains("- Y.sol"));
        assert!(!output.contains('+'));
        assert!(!output.contains('~'));
    }

    #[test]
    fn renders_modified_with_address_change() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "migrated": {
                        "address": "0x2222222222222222222222222222222222222222",
                        "name": "Same Name",
                        "changeType": "modified",
                        "metadataChanges": {
                            "address": {
                                "from": "0x1111111111111111111111111111111111111111",
                                "to": "0x2222222222222222222222222222222222222222"
                            }
                        },
                        "assertions": []
                    }
                },
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 1, "unchanged": 0 },
                    "assertions": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("~ contract \"migrated\" (0x2222...2222)"));
        assert!(output.contains("address: 0x1111...1111 \u{2192} 0x2222...2222"));
        assert!(!output.contains("name:"));
    }

    #[test]
    fn renders_modified_with_compiler_version_change() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "upgraded": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "name": "U",
                        "changeType": "modified",
                        "metadataChanges": null,
                        "assertions": [{
                            "file": "Upgraded.sol",
                            "args": [],
                            "changeType": "modified",
                            "assertionId": "0x1",
                            "previousAssertionId": "0x0",
                            "compilerVersionChange": { "from": "0.8.20", "to": "0.8.24" }
                        }]
                    }
                },
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 1, "unchanged": 0 },
                    "assertions": { "added": 0, "removed": 0, "modified": 1, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("~ Upgraded.sol"));
        assert!(output.contains("compiler: 0.8.20 \u{2192} 0.8.24"));
    }

    #[test]
    fn renders_only_unchanged() {
        disable_colors();

        let json = json!({
            "hasChanges": false,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "stable_a": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "name": "A", "changeType": "unchanged",
                        "metadataChanges": null, "assertions": []
                    },
                    "stable_b": {
                        "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "name": "B", "changeType": "unchanged",
                        "metadataChanges": null, "assertions": []
                    }
                },
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 0, "unchanged": 2 },
                    "assertions": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("contract \"stable_a\" (unchanged)"));
        assert!(output.contains("contract \"stable_b\" (unchanged)"));
        assert!(!output.contains('+'));
        assert!(!output.contains('-'));
        assert!(!output.contains('~'));
    }

    #[test]
    fn renders_empty_diff() {
        disable_colors();

        let json = json!({
            "hasChanges": false,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {},
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 },
                    "assertions": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("Plan: 0 added, 0 changed, 0 removed."));
    }

    #[test]
    fn renders_modified_name_only() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "renamed": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "name": "New Name",
                        "changeType": "modified",
                        "metadataChanges": {
                            "name": { "from": "Old Name", "to": "New Name" }
                        },
                        "assertions": []
                    }
                },
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 1, "unchanged": 0 },
                    "assertions": { "added": 0, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("~ contract \"renamed\" (0xaaaa...aaaa)"));
        assert!(output.contains("name: \"Old Name\" \u{2192} \"New Name\""));
        assert!(!output.contains("address:"));
    }

    #[test]
    fn renders_modified_with_mixed_assertion_changes() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "tweaked": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "name": "Same",
                        "changeType": "modified",
                        "metadataChanges": null,
                        "assertions": [
                            { "file": "New.sol", "args": [], "changeType": "added", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null },
                            { "file": "Old.sol", "args": [], "changeType": "removed", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null }
                        ]
                    }
                },
                "summary": {
                    "contracts": { "added": 0, "removed": 0, "modified": 1, "unchanged": 0 },
                    "assertions": { "added": 1, "removed": 1, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("~ contract \"tweaked\""));
        assert!(output.contains("+ New.sol"));
        assert!(output.contains("- Old.sol"));
        assert!(!output.contains("name:"));
        assert!(!output.contains("address:"));
    }

    #[test]
    fn renders_multiple_added_contracts() {
        disable_colors();

        let json = json!({
            "hasChanges": true,
            "configMismatch": false,
            "driftDetected": false,
            "diff": {
                "contracts": {
                    "alpha": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "name": "A", "changeType": "added",
                        "metadataChanges": null,
                        "assertions": [
                            { "file": "A.sol", "args": [], "changeType": "added", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null }
                        ]
                    },
                    "beta": {
                        "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "name": "B", "changeType": "added",
                        "metadataChanges": null,
                        "assertions": [
                            { "file": "B1.sol", "args": [], "changeType": "added", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null },
                            { "file": "B2.sol", "args": [], "changeType": "added", "assertionId": null, "previousAssertionId": null, "compilerVersionChange": null }
                        ]
                    }
                },
                "summary": {
                    "contracts": { "added": 2, "removed": 0, "modified": 0, "unchanged": 0 },
                    "assertions": { "added": 3, "removed": 0, "modified": 0, "unchanged": 0 }
                }
            },
            "diffedAgainstReleaseId": null
        });

        let preview: PreviewResponse = serde_json::from_value(json).unwrap();
        let output = preview.render_plan();
        assert!(output.contains("+ contract \"alpha\" (0xaaaa...aaaa)"));
        assert!(output.contains("+ contract \"beta\" (0xbbbb...bbbb)"));
        assert!(output.contains("+ B1.sol"));
        assert!(output.contains("+ B2.sol"));
    }

    #[test]
    fn abbreviates_long_hex_address() {
        assert_eq!(
            abbreviate_address("0x1234567890abcdef1234567890abcdef12345678"),
            "0x1234...5678"
        );
    }

    #[test]
    fn preserves_short_address() {
        assert_eq!(abbreviate_address("0xshort"), "0xshort");
    }

    #[test]
    fn preserves_non_hex_string() {
        assert_eq!(abbreviate_address("localhost"), "localhost");
    }
}
