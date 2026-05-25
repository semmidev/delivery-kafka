# Delivery App — Kafka Production Readiness Checklist

This document is an analysis of the current Kafka setup (`compose.yml`, `init-topics.sh`) and Go client usage (`producer/kafka.go`, `consumer/kafka.go`). While the current setup is excellent for a robust local/staging environment and demonstrates many High Availability (HA) best practices, there are several critical gaps that must be addressed before migrating to a true production environment.

## 1. Infrastructure & Cluster Setup (KRaft)

Currently, `compose.yml` runs a 3-node KRaft cluster in "combined" mode (each node is both a broker and a controller) using plain Docker volumes.

- [ ] **Separate Controllers & Brokers**: In production, it is highly recommended to separate the Controller nodes from the Broker nodes. Controllers handle cluster metadata and shouldn't be impacted by high I/O broker operations. Use 3 dedicated Controllers and 3+ dedicated Brokers.
- [ ] **Storage & Disks**: Docker local volumes are not sufficient. You must provision high-IOPS SSDs (e.g., AWS EBS gp3 or io2, or NVMe local disks). Format disks with `XFS` or `ext4` and mount them exclusively for Kafka logs (`/var/lib/kafka/data`).
- [ ] **Memory & JVM Tuning**: There are no `KAFKA_HEAP_OPTS` defined. By default, Kafka relies heavily on the OS Page Cache. Set the JVM heap to 4GB-6GB max (`-Xmx6G -Xms6G`), leaving the rest of the machine's RAM for the OS to cache disk I/O.
- [ ] **Multi-AZ Deployment**: Ensure that brokers are spread out across multiple Availability Zones (AZs) or racks. Configure `broker.rack` in Kafka properties so that replicas of the same partition are never placed in the same physical rack/AZ.

## 2. Security (Network & Auth)

Currently, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` is set to `PLAINTEXT` for all listeners, meaning data is unencrypted and anyone can connect without a password.

- [ ] **Enable TLS/SSL Encryption**: Change external listeners to use `SSL` or `SASL_SSL` to encrypt traffic in transit between the Go microservices and the Kafka cluster.
- [ ] **Enable SASL Authentication**: Implement SASL/SCRAM or mTLS (Mutual TLS) so that only authorized clients (your producer/consumer) can read/write to the cluster.
- [ ] **Network Isolation / VPC**: Do not bind `EXTERNAL` listeners to public IPs `0.0.0.0` unless protected by strict security groups. Deploy Kafka within a private VPC subnet.
- [ ] **Kafka ACLs**: Enable Access Control Lists (ACLs). The consumer service should only have `READ` access to specific topics, and the producer should only have `WRITE` access.

## 3. Client Usage (Go / `franz-go`)

Your client configurations are mostly solid (using idempotent producers, Lz4 compression, explicit consumer groups, and manual commits). However, a few critical adjustments are needed:

- [ ] **Synchronous DLQ Handling**: In `consumer/kafka.go`, when a message fails parsing, you send it to the DLQ asynchronously (`client.Produce(ctx, dlqRecord, callback)`), and then return `nil` to immediately commit the offset. **Risk:** If the DLQ broker is down, the async produce fails, you log the error, but the offset is still committed. The failed message is permanently lost.
  - *Fix:* Wait for the DLQ `Produce` promise to resolve before returning, OR only commit offsets after ensuring DLQ writes were successful.
- [ ] **Offset Reset Strategy**: The consumer uses `kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())`. If you deploy a new consumer group in production, it will process *all* historical data (up to 30 days based on retention).
  - *Fix:* Evaluate if you want new groups to start from `AtEnd()` (only new live data) or explicitly handle the massive initial load if using `AtStart()`.
- [ ] **Security Configs in Go**: Update `kgo.NewClient(...)` to inject TLS certificates (`kgo.DialTLS`) and SASL credentials (`kgo.SASL`) matching your cluster's new security setup.
- [ ] **Concurrent Processing**: The current `runConsumerLoop` processes messages strictly sequentially. If throughput spikes, you may experience consumer lag.
  - *Fix:* Implement worker pools for message processing, but be extremely careful to track offsets properly (only commit the lowest contiguous processed offset).

## 4. Observability & Monitoring

Kafbat UI is great for manual inspection, but you need automated alerts for production.

- [ ] **JMX Metrics Exporter**: Kafka exposes critical metrics via JMX. Run a JMX Prometheus Exporter alongside each broker.
- [ ] **Dashboards (Grafana)**: Build dashboards tracking:
  - `UnderReplicatedPartitions` (URP) - **Must alert immediately if > 0**
  - `OfflinePartitionsCount`
  - `ActiveControllerCount` (Should exactly equal 1 across the cluster)
  - `NetworkProcessorAvgIdlePercent` / `RequestHandlerAvgIdlePercent`
- [ ] **Consumer Lag Monitoring**: Monitor the lag of `tracking-api-service`. If lag continuously grows, the consumer is too slow or down.
- [ ] **Client Metrics**: Export `franz-go` client metrics (produce latency, fetch latency, error rates) to your monitoring stack.

## 5. Backup & Disaster Recovery

Kafka is a distributed log, not a traditional database. "Backing up" Kafka requires different strategies.

- [ ] **Tiered Storage**: Consider enabling Kafka Tiered Storage (KIP-405, available in recent Kafka versions) to offload older log segments to Amazon S3 / GCS automatically. This saves expensive SSD space and acts as a long-term backup.
- [ ] **Cross-Region Replication**: If you need multi-region HA, set up **MirrorMaker 2 (MM2)** to replicate topics asynchronously from your primary cluster to a standby cluster in another geographic region.
- [ ] **Configuration Backup**: Back up your topic definitions, ACLs, and cluster configs. Your `init-topics.sh` is a good start, but in production, use declarative tools like Terraform or Strimzi (if on Kubernetes) to manage topic state.
