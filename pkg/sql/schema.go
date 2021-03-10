package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

var initializeSchemaMu sync.Mutex

func initializeSchema(
	ctx context.Context,
	topic string,
	logger watermill.LoggerAdapter,
	db contextExecutor,
	schemaAdapter SchemaAdapter,
	offsetsAdapter OffsetsAdapter,
	consumerGroup string,
) error {
	fmt.Println("==> schema enter")
	initializeSchemaMu.Lock()
	fmt.Println("==> schema lock")
	defer fmt.Println("==> schema unlock")
	defer initializeSchemaMu.Unlock()

	err := validateTopicName(topic)
	if err != nil {
		return err
	}

	initializingQueries := schemaAdapter.SchemaInitializingQueries(topic)
	if offsetsAdapter != nil {
		initializingQueries = append(initializingQueries, offsetsAdapter.SchemaInitializingQueries(topic, consumerGroup)...)
	}

	logger.Info("Initializing subscriber schema", watermill.LogFields{
		"query": initializingQueries,
	})

	for _, q := range initializingQueries {
		fmt.Println("==>", q)
		_, err := db.ExecContext(ctx, q)
		if err != nil {
			return errors.Wrap(err, "could not initialize schema")
		}
	}

	return nil
}
