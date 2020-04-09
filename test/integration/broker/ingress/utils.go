package ingress

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	lib "github.com/google/knative-gcp/cmd/broker/ingress/lib"
	"github.com/google/knative-gcp/pkg/broker/config"
	volumetest "github.com/google/knative-gcp/pkg/broker/config/volume/test"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var (
	broker1 = &config.Broker{
		Id:            "b-uid-1",
		Name:          "broker1",
		Namespace:     "ns1",
		DecoupleQueue: &config.Queue{Topic: "test-broker1-" + uuid.New().String()},
	}
	broker2 = &config.Broker{
		Id:            "b-uid-2",
		Name:          "broker2",
		Namespace:     "ns2",
		DecoupleQueue: &config.Queue{Topic: "test-broker2-" + uuid.New().String()},
	}
)

type runner struct {
	clients  map[string]cloudevents.Client
	cleanups []func()
	env      lib.EnvConfig
	subs     map[string]*pubsub.Subscription
}

func setUp(tb testing.TB, brokers ...*config.Broker) *runner {
	r := &runner{
		clients:  make(map[string]cloudevents.Client),
		cleanups: make([]func(), 0),
		subs:     make(map[string]*pubsub.Subscription),
	}
	ctx := context.Background()

	// create broker config file
	brokerConfig := createBrokerConfig(brokers...)
	fmt.Printf("\nTesting with brokerconfig: %+v \n", brokerConfig)
	path, cleanup := volumetest.CreateConfigFile(tb, brokerConfig)
	// Set the config path env.
	os.Setenv("BROKER_CONFIG_PATH", path)
	r.cleanups = append(r.cleanups, cleanup)

	r.env = lib.ProcessEnvVar()

	// create pubsub topic and subscriptions
	pc, err := pubsub.NewClient(ctx, r.env.ProjectID)
	if err != nil {
		tb.Fatalf("Failed to create pubsub client: %v", err)
	}
	for _, b := range brokerConfig.Brokers {
		topic, err := pc.CreateTopic(ctx, b.DecoupleQueue.Topic)
		if err != nil {
			tb.Fatalf("Failed to create topic: %v", err)
		}
		r.cleanups = append(r.cleanups, func() { pc.Topic(b.DecoupleQueue.Topic).Delete(ctx) })
		sub, err := pc.CreateSubscription(ctx, subFor(b.DecoupleQueue.Topic), pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			tb.Fatalf("failed to create subscription: %v", err)
		}
		r.subs[config.BrokerKey(b.Namespace, b.Name)] = sub
		r.cleanups = append(r.cleanups, func() { pc.Subscription(subFor(b.DecoupleQueue.Topic)).Delete(ctx) })
	}

	// prepare client to send event
	for k := range brokerConfig.Brokers {
		r.clients[k] = NewHTTPClient(tb, "http://localhost:"+strconv.Itoa(r.env.Port)+"/"+k)
	}

	// TODO prepare client to receive event

	return r
}

func subFor(topic string) string {
	return "sub-for-" + topic
}

func (r *runner) tearDown() {
	for _, cleanup := range r.cleanups {
		cleanup()
	}

	// remove broker config file

	// delete pubsub pull subscriptions

	// delete pubsub topics
}

func waitIngressReady(client cloudevents.Client, timeout time.Duration) bool {
	timer := time.After(timeout)
	for {
		select {
		case <-timer:
			return false
		default:
			res := client.Send(context.Background(), *createTestEvent("test-id"))
			if cloudevents.IsACK(res) {
				return true
			}
		}
		time.Sleep(time.Second)
	}
}

func createBrokerConfig(brokers ...*config.Broker) *config.TargetsConfig {
	brokerConfig := &config.TargetsConfig{
		Brokers: make(map[string]*config.Broker),
	}
	for _, b := range brokers {
		brokerConfig.Brokers[config.BrokerKey(b.Namespace, b.Name)] = b
	}
	return brokerConfig
}

func NewHTTPClient(tb testing.TB, url string) cloudevents.Client {
	p, err := cloudevents.NewHTTP(cloudevents.WithTarget(url))
	if err != nil {
		tb.Fatalf("Failed to create HTTP protocol: %+v", err)
	}
	ce, err := cloudevents.NewClient(p)
	if err != nil {
		tb.Fatalf("Failed to create HTTP client: %+v", err)
	}
	return ce
}

func createTestEvent(id string) *cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetID(id)
	event.SetSource("test-source")
	event.SetType("test-type")
	return &event
}

func newPubsubConn() {

}

func newPubsubClient(tb testing.TB, ctx context.Context, projectID string, conn *grpc.ClientConn) (*pubsub.Client, func()) {
	tb.Helper()
	c, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn))
	if err != nil {
		tb.Fatalf("failed to create test pubsub client: %v", err)
	}
	return c, func() {
		c.Close()
		conn.Close()
	}
}

func testPubsubConn(ctx context.Context, tb testing.TB, projectID string) (*grpc.ClientConn, func()) {
	tb.Helper()
	srv := pstest.NewServer()
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		tb.Fatalf("failed to dial test pubsub connection: %v", err)
	}
	return conn, func() {
		srv.Close()
	}
}
