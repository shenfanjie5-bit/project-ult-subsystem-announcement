# subsystem-announcement

Announcement-domain Ex producer. It discovers official announcement
documents, parses supported attachments through Docling, extracts
evidence-backed announcement facts/signals/graph deltas, and submits
contract payloads through subsystem-sdk.

Source of truth:

- `docs/subsystem-announcement.project-doc.md`

## Current state

- Docling boundary exists for `pdf`, `html`, and `word` announcement
  attachments.
- Ex-1/Ex-2/Ex-3 producer models and canonical wire mapping exist.
- Deterministic announcement extractors cover earnings preannounce, major
  contract, shareholder change, equity pledge, regulatory action,
  trading halt/resume, and fundraising change.
- High-threshold Ex-3 is intentionally narrow: control/shareholding changes,
  supply-contract/cooperation edges, and evidence-guarded relations only.

## M4.7 document validation gate

M4.7 remains **partial**. The current Docling/LlamaIndex evidence is a
synthetic preflight artifact and does not satisfy the production criterion of
10-20 representative A-share documents parsed offline. Do not treat
financial-report extraction as unlocked until the real-document proof exists.

Financial-doc planning boundary:

- Do not create a second parser or a parallel Docling stack.
- Do not start an immediate `subsystem-financial-doc` repository.
- Future financial-doc work must reuse this Docling boundary or a shared
  common doc-pipeline extracted from it.
- Financial-doc scope, when unlocked, is limited to periodic-report /
  prospectus tables such as top customers, top suppliers, related parties,
  and IPO project follow-through. Announcement events stay here.

Execution rule:

1. read the project doc first
2. keep work inside this module unless the issue explicitly targets shared contracts
3. do not introduce another parser; Docling remains the single document
   parsing frontend
