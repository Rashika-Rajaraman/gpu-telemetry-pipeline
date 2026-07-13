package wire

// This file defines the JSON payload for each opcode. []byte fields marshal as
// base64 in JSON, so binary keys/values are transported safely.

// ProduceReq is sent by a producer to append a message to a topic. Key selects
// the partition (hash(Key) % N); records sharing a key preserve their order.
type ProduceReq struct {
	Topic string `json:"topic"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value"`
}

// ProduceAck acknowledges a produced message with its assigned location, or
// reports an error string if the append failed.
type ProduceAck struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error,omitempty"`
}

// SubscribeReq is sent by a consumer to join a group and subscribe to a topic.
type SubscribeReq struct {
	Group      string `json:"group"`
	Topic      string `json:"topic"`
	ConsumerID string `json:"consumer_id"`
}

// AssignMsg tells a consumer which partitions it currently owns. The broker sends
// it on join and on every rebalance.
type AssignMsg struct {
	Partitions []int `json:"partitions"`
}

// Record is a single message delivered to a consumer.
type Record struct {
	Topic             string `json:"topic"`
	Partition         int    `json:"partition"`
	Offset            int64  `json:"offset"`
	Key               []byte `json:"key,omitempty"`
	Value             []byte `json:"value"`
	TimestampUnixNano int64  `json:"ts"`
}

// DeliverMsg carries a batch of records from the broker to a consumer.
type DeliverMsg struct {
	Records []Record `json:"records"`
}

// AckReq commits processed offsets per partition for a consumer group. Offsets is
// keyed by partition id; the value is the next offset the consumer expects (i.e.
// everything below it is acknowledged).
type AckReq struct {
	Group   string          `json:"group"`
	Offsets map[int]int64   `json:"offsets"`
}

// Heartbeat keeps a consumer's group membership alive between deliveries.
type Heartbeat struct {
	Group      string `json:"group"`
	ConsumerID string `json:"consumer_id"`
}

// ErrorMsg reports a broker-side error to a client.
type ErrorMsg struct {
	Message string `json:"message"`
}
