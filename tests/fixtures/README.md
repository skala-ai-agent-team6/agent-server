# Backend v2 schema fixture

Source: https://github.com/skala-ai-agent-team6/backend/blob/63e0f968f76c893ec9554589d07b51a81e0460ef/docs/kafka-event.schema.json

Copied unchanged for offline contract tests. Snapshot commit: 63e0f968f76c893ec9554589d07b51a81e0460ef.
The v2 tests validate actual output against this schema, including UUID/date-time formats.
Synthetic command attachments are explicitly categorized to test the codec only; the real backend currently stores GENERAL.
