package sql_test

// func TestValidateTopicName(t *testing.T) {
// 	schemaAdapter := sql.DefaultMySQLSchema{}
// 	offsetsAdapter := sql.DefaultMySQLOffsetsAdapter{}

// 	publisher, subscriber := newPubSub(t, newMySQL(t), "", schemaAdapter, offsetsAdapter)
// 	cleverlyNamedTopic := "some_topic; DROP DATABASE `watermill`"

// 	err := publisher.Publish(cleverlyNamedTopic, message.NewMessage("uuid", nil))
// 	require.Error(t, err)
// 	assert.Equal(t, sql.ErrInvalidTopicName, errors.Cause(err))

// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	_, err = subscriber.Subscribe(ctx, cleverlyNamedTopic)
// 	require.Error(t, err)
// 	assert.Equal(t, sql.ErrInvalidTopicName, errors.Cause(err))
// }
