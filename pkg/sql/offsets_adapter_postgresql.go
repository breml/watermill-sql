package sql

import (
	"fmt"
	"strings"
)

// DefaultPostgreSQLOffsetsAdapter is adapter for storing offsets in PostgreSQL database.
//
// DefaultPostgreSQLOffsetsAdapter is designed to support multiple subscribers with exactly once delivery
// and guaranteed order.
//
// We are using FOR UPDATE in NextOffsetQuery to lock consumer group in offsets table.
//
// When another consumer is trying to consume the same message, deadlock should occur in ConsumedMessageQuery.
// After deadlock, consumer will consume next message.
type DefaultPostgreSQLOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultPostgreSQLOffsetsAdapter) SchemaInitializingQueries(topic, consumerGroup string) []string {
	if consumerGroup == "" {
		consumerGroup = "__default__"
	}

	// aquireAdvisoryLock := `SELECT pg_advisory_lock(('x'||substr(md5('` + consumerGroup + `'),1,16))::bit(64)::bigint);
	// `

	createConsumerGroupOffsetsTable := `CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic+`_`+consumerGroup) + ` (
		"offset" BIGINT NOT NULL PRIMARY KEY,
		"acked_at" TIMESTAMP
	);
	`

	addConsumerGroupToTopic := `INSERT INTO ` + strings.ReplaceAll(a.MessagesOffsetsTable(topic+`_consumer_groups`), `_offsets`, ``) + ` (consumer_group) values ('` + consumerGroup + `') ON CONFLICT DO NOTHING;
	`

	// releaseAdvisoryLock := `SELECT pg_advisory_unlock(('x'||substr(md5('` + consumerGroup + `'),1,16))::bit(64)::bigint);
	// `

	return []string{
		// aquireAdvisoryLock,
		createConsumerGroupOffsetsTable,
		addConsumerGroupToTopic,
		// releaseAdvisoryLock,
	}
}

func (a DefaultPostgreSQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	if consumerGroup == "" {
		consumerGroup = "__default__"
	}
	return `SELECT COALESCE(
				(SELECT "offset"
				 FROM ` + a.MessagesOffsetsTable(topic+`_`+consumerGroup) + `
				 WHERE acked_at IS NULL ORDER BY "offset" ASC LIMIT 1 FOR UPDATE SKIP LOCKED
				), 0)`,
		[]interface{}{}
}

func (a DefaultPostgreSQLOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	if consumerGroup == "" {
		consumerGroup = "__default__"
	}
	ackQuery := `UPDATE ` + a.MessagesOffsetsTable(topic+`_`+consumerGroup) + ` SET acked_at = now() WHERE "offset" = $1`
	return ackQuery, []interface{}{offset}
}

func (a DefaultPostgreSQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf(`"watermill_offsets_%s"`, topic)
}

func (a DefaultPostgreSQLOffsetsAdapter) ConsumedMessageQuery(
	topic string,
	offset int,
	consumerGroup string,
	consumerULID []byte,
) (string, []interface{}) {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES ($1, $2) ON CONFLICT("consumer_group") DO UPDATE SET offset_consumed=excluded.offset_consumed`
	return ackQuery, []interface{}{offset, consumerGroup}
}
