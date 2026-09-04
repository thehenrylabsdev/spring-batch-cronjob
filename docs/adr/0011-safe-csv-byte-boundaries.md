# ADR-0011: Pre-index safe CSV byte boundaries

## Status

Accepted

## Decision

The coordinator scans UTF-8 bytes once with a quote-aware state machine. It hashes, validates, counts logical
records, identifies the header end, and plans only logical-record boundaries. Readers seek to persisted offsets.

## Consequences

Quoted CRLF/LF multiline fields never become a partition boundary. Memory grows with partition count, not record
count. The deliberate sequential index pass avoids every pod rescanning earlier input bytes.
