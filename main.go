package main

import (
	"context"
	"fmt"
	"sync"

	octrace "go.opencensus.io/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

const name = "app"

func initTracer() (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint("127.0.0.1:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(name))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	bridge := otel.GetTracerProvider().Tracer("opencensus-bridge")
	octrace.DefaultTracer = opencensus.NewTracer(bridge)
	return tp, nil
}

func main() {
	tp, err := initTracer()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}()

	db, err := newDBClient()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := run(ctx, db); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, conn *spannerConnection) error {
	newCtx, span := otel.Tracer(name).Start(ctx, "Run")
	defer span.End()

	conn.GetUserByID(newCtx, "199f8059-558a-4c6f-aad3-526859cfa88e")

	waitNum := 10
	var wg sync.WaitGroup
	for i := 0; i < waitNum; i++ {
		fmt.Printf("------- query: UpdateCounter (%d) -------\n", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.IncrementCounterByID(newCtx, "05a7f30c-823c-4502-a866-6ac783050e4f")
		}()
	}
	wg.Wait()

	return nil
}
