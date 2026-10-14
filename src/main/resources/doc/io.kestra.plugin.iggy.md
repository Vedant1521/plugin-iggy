# How to use the Apache Iggy plugin

Publish and consume messages on Apache Iggy streams and topics, track consumer offsets, and react to streaming events from Kestra flows.

## Common properties

Set `host` and `port` to your Apache Iggy server connection details (default port `8090` for TCP, `3000` for HTTP). Set `transport` to `TCP` (default) or `HTTP`. For authentication, provide `username` and `password` or a personal access `token`. Enable TLS via `tls: true`. Sensitive values should be stored in [secrets](https://kestra.io/docs/concepts/secret).

## Tasks

- `Produce` publishes messages to an Apache Iggy `stream` and `topic` — pass message data via `from`. Messages can include `payload`, optional `key`, and `headers` map. Supports inline list of messages, map, or internal storage URI.
- `Consume` reads messages from an Apache Iggy `stream` and `topic`. Set `consumerId` to identify the consumer, `partitionId` to target a partition, and bound the batch with `maxRecords` or `maxDuration`. Messages are stored in Kestra internal storage.

## Triggers

- `Trigger` polls on a schedule (default 60 seconds) and starts one execution per batch of messages.
- `RealtimeTrigger` listens to new messages in real-time and starts one execution per message as it arrives.
