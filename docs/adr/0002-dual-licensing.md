# ADR 0002: Re-licensing to Dual MIT / Apache-2.0

## Status
Accepted

## Context
`mqtt-async-embedded` was originally released under the GNU General Public License v3.0 (GPL-3.0). While GPL-3.0 ensures open copyleft terms, it acts as a severe barrier to commercial and industrial adoption in the embedded, IoT, robotics, and firmware communities where linking proprietary device drivers and closed-source sensor control logic is standard practice.

## Decision
Re-license all crates in the workspace under standard dual licensing:
- **MIT License** ([LICENSE-MIT](file:///home/jrad/RustroverProjects/mqtt-async-embedded/LICENSE-MIT))
- **Apache License, Version 2.0** ([LICENSE-APACHE](file:///home/jrad/RustroverProjects/mqtt-async-embedded/LICENSE-APACHE))

## Consequences
- **Positive**:
  - Removes all legal blockers for enterprise, industrial IoT, and embedded firmware deployment.
  - Aligns with the standard Rust ecosystem licensing convention (Rust compiler, Tokio, Embassy, serde).
  - Encourages corporate sponsorship, third-party contributions, and wider adoption.
- **Negative**:
  - Code can be incorporated into proprietary commercial systems without requiring open-sourcing derived applications.
