package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultPostgreSQLSchema is a default implementation of SchemaAdapter based on PostgreSQL.
type DefaultPostgreSQLSchema struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string
}

func (s DefaultPostgreSQLSchema) SchemaInitializingQueries(topic string) []string {
	// aquireAdvisoryLock := `SELECT pg_advisory_lock(('x'||substr(md5('` + topic + `'),1,16))::bit(64)::bigint);
	// `

	createMessagesTable := `CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(topic) + ` (
		"offset" SERIAL PRIMARY KEY,
		"uuid" VARCHAR(36) NOT NULL,
		"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		"payload" bytea DEFAULT NULL,
		"metadata" JSON DEFAULT NULL
	);
	`

	createConsumerGroupsTable := `CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(topic+`_consumer_groups`) + ` (
		"consumer_group" varchar(64) NOT NULL PRIMARY KEY
	);
	`

	createConsumerGroupsUpdateFunction := `CREATE OR REPLACE FUNCTION ` + strings.ReplaceAll(strings.ReplaceAll(s.MessagesTable(topic+`_cg_update`), `"`, ``), "-", "") + `() RETURNS TRIGGER AS $$
		DECLARE
		query TEXT ;
		consumergroup VARCHAR;
		BEGIN
			FOR consumergroup IN
				SELECT consumer_group FROM ` + s.MessagesTable(topic+`_consumer_groups`) + `
				LOOP
					query := CONCAT('INSERT INTO ', FORMAT('%I', CONCAT('` + strings.ReplaceAll(s.MessagesTable(`offsets_`+topic+`_`), `"`, ``) + `', consumergroup)), ' ("offset") values (', new.offset, ')');
					EXECUTE query;
				END LOOP;
			RETURN NULL;
		END;
	$$ language plpgsql;
	`

	dropCreateConsumerGroupUpdateTriggerIfExists := `DROP TRIGGER IF EXISTS ` + s.MessagesTable(topic+"_consumer_groups_trigger") + ` ON ` + s.MessagesTable(topic) + `;
	`

	createConsumerGroupUpdateTrigger := `CREATE TRIGGER ` + s.MessagesTable(topic+"_consumer_groups_trigger") + `
		AFTER INSERT ON ` + s.MessagesTable(topic) + ` FOR EACH ROW EXECUTE PROCEDURE ` + strings.ReplaceAll(strings.ReplaceAll(s.MessagesTable(topic+`_cg_update`), `"`, ``), `-`, ``) + `();
	`

	// releaseAdvisoryLock := `SELECT pg_advisory_unlock(('x'||substr(md5('` + topic + `'),1,16))::bit(64)::bigint);
	// `

	return []string{
		// aquireAdvisoryLock,
		createMessagesTable,
		createConsumerGroupsTable,
		createConsumerGroupsUpdateFunction,
		dropCreateConsumerGroupUpdateTriggerIfExists,
		createConsumerGroupUpdateTrigger,
		// releaseAdvisoryLock,
	}
}

func (s DefaultPostgreSQLSchema) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(topic),
		defaultInsertMarkers(len(msgs)),
	)

	args, err := defaultInsertArgs(msgs)
	if err != nil {
		return "", nil, err
	}

	return insertQuery, args, nil
}

func defaultInsertMarkers(count int) string {
	result := strings.Builder{}

	index := 1
	for i := 0; i < count; i++ {
		result.WriteString(fmt.Sprintf("($%d,$%d,$%d),", index, index+1, index+2))
		index += 3
	}

	return strings.TrimRight(result.String(), ",")
}

func (s DefaultPostgreSQLSchema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) (string, []interface{}) {
	nextOffsetQuery, nextOffsetArgs := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)
	selectQuery := `
		SELECT "offset", uuid, payload, metadata FROM ` + s.MessagesTable(topic) + `
		WHERE
			"offset" = (` + nextOffsetQuery + `)
		ORDER BY
			"offset" ASC
		LIMIT 1`

	return selectQuery, nextOffsetArgs
}

func (s DefaultPostgreSQLSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	return unmarshalDefaultMessage(row)
}

func (s DefaultPostgreSQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}
