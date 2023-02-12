package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/otel"
	"google.golang.org/api/iterator"
)

const (
	SPANNER_PROJECT  = "zerocloc-dev-test-env"
	SPANNER_INSTANCE = "sqlc-test"
	SPANNER_DATABSE  = "sqlc-test"
)

type User struct {
	ID   string
	Name string
}

type Counter struct {
	ID    string
	Count int64
}

type spannerConnection struct {
	*spanner.Client
}

func (c *spannerConnection) GetUserByID(ctx context.Context, userID string) error {
	newCtx, span := otel.Tracer(name).Start(ctx, "GetUserByID")
	defer span.End()

	stmt := spanner.NewStatement("SELECT * FROM Users WHERE ID = @userID")
	stmt.Params["userID"] = userID
	iter := c.Single().Query(newCtx, stmt)

	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		fmt.Println("No data found.")
		return nil
	}
	if err != nil {
		return err
	}

	user := &User{}
	if err := row.ToStruct(user); err != nil {
		return err
	}
	fmt.Printf("User(ID: %s): %v\n", userID, user)

	return nil
}

func (c *spannerConnection) IncrementCounterByID(ctx context.Context, counterID string) error {
	newCtx, span := otel.Tracer(name).Start(ctx, "IncrementCounterByID")
	defer span.End()

	_, err := c.ReadWriteTransaction(newCtx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		stmt := spanner.NewStatement("SELECT * FROM Counters WHERE ID = @counterID")
		stmt.Params["counterID"] = counterID
		iter := tx.Query(ctx, stmt)
		defer iter.Stop()
		row, err := iter.Next()
		if err == iterator.Done {
			fmt.Println("No data found.")
			return nil
		}
		if err != nil && err != iterator.Done {
			return err
		}
		counter := &Counter{}
		if row != nil {
			if err := row.ToStruct(counter); err != nil {
				return err
			}
		}

		counter.Count++
		m, err := spanner.UpdateStruct("Counters", counter)
		if err != nil {
			return nil
		}
		err = tx.BufferWrite([]*spanner.Mutation{m})
		if err != nil {
			return nil
		}

		return nil
	})

	return err
}

func newDBClient() (*spannerConnection, error) {
	db := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		SPANNER_PROJECT,
		SPANNER_INSTANCE,
		SPANNER_DATABSE,
	)
	c, err := spanner.NewClient(context.Background(), db)
	if err != nil {
		return nil, err
	}

	return &spannerConnection{c}, nil
}
