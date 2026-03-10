---
name: samply-profile
description: Profile Rust→find CPU hotspots→ask user
---

⚙ samply-profile: Rust CPU hotspot finder

① `RUSTFLAGS="-C debuginfo=1" cargo build --release`
② `mkdir -p artifacts/profiles && samply record --save-only -o artifacts/profiles/profile.json.gz -- ./target/release/⟨bin⟩ ⟨args⟩`
③ `samply load artifacts/profiles/profile.json.gz`
④ Analyze flamegraph:
   wide blocks ≈ ⊤ → ↑CPU
   σ(self)↑ → optim fn body
   Σ(total)↑ ∧ σ↓ → optim callees ∨ ↓call freq
   proj crates ≫ runtime∕sys frames
⑤ Report findings → ask user ∀ next steps. ¬optimize w∕o permission.

macOS 1st run: `samply setup -y`
Attach: `samply record -p ⟨pid⟩ -d 30 --save-only -o artifacts/profiles/profile.json.gz`
