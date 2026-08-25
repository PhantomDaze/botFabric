package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type testPolicy struct {
	allowInvoke  bool
	allowProvide bool
}

func (p testPolicy) AllowPublish(string, string) bool        { return true }
func (p testPolicy) AllowSubscribe(string, string) bool      { return true }
func (p testPolicy) AllowInvoke(string, string, string) bool { return p.allowInvoke }
func (p testPolicy) AllowProvide(string, string) bool        { return p.allowProvide }

func TestInvokeOverwritesClaimedCaller(t *testing.T) {
	gateway := newTestGateway(t, testPolicy{allowInvoke: true, allowProvide: true}, time.Second)
	var receivedCaller string
	descriptor := contract.ServiceDescriptor{Name: "example.service", Methods: []string{"Run"}}
	provider := gateway.ForPlugin("provider")
	if err := provider.Register(descriptor, func(_ context.Context, req contract.Request) (contract.Response, error) {
		receivedCaller = req.Caller
		return contract.Response{Payload: []byte("ok")}, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := gateway.ForPlugin("trusted-caller").Invoke(context.Background(), contract.Request{
		Service: descriptor.Name,
		Method:  "Run",
		Caller:  "forged-caller",
	})
	if err != nil {
		t.Fatal(err)
	}
	if receivedCaller != "trusted-caller" {
		t.Fatalf("处理器收到 Caller = %q，期望可信身份", receivedCaller)
	}
}

func TestInvokeDeniedBeforeServiceLookup(t *testing.T) {
	gateway := newTestGateway(t, testPolicy{}, time.Second)
	response, err := gateway.ForPlugin("caller").Invoke(context.Background(), contract.Request{
		Service: "secret.service",
		Method:  "Read",
	})
	var serviceError *contract.ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != contract.ErrorPermissionDenied {
		t.Fatalf("错误 = %v，期望 permission_denied", err)
	}
	if response.Error == nil || response.Error.Code != contract.ErrorPermissionDenied {
		t.Fatalf("响应错误 = %#v，期望 permission_denied", response.Error)
	}
}

func TestInvokePreservesServiceError(t *testing.T) {
	gateway := newTestGateway(t, testPolicy{allowInvoke: true, allowProvide: true}, time.Second)
	descriptor := contract.ServiceDescriptor{Name: "validated.service", Methods: []string{"Run"}}
	if err := gateway.ForPlugin("provider").Register(descriptor, func(context.Context, contract.Request) (contract.Response, error) {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "载荷无效"}
	}); err != nil {
		t.Fatal(err)
	}
	response, err := gateway.ForPlugin("caller").Invoke(context.Background(), contract.Request{Service: descriptor.Name, Method: "Run"})
	var serviceError *contract.ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != contract.ErrorInvalidArgument {
		t.Fatalf("错误 = %v，期望 invalid_argument", err)
	}
	if response.Error == nil || response.Error.Code != contract.ErrorInvalidArgument {
		t.Fatalf("响应错误 = %#v", response.Error)
	}
}

func TestInvokeTimeout(t *testing.T) {
	gateway := newTestGateway(t, testPolicy{allowInvoke: true, allowProvide: true}, 20*time.Millisecond)
	descriptor := contract.ServiceDescriptor{Name: "slow.service", Methods: []string{"Run"}}
	if err := gateway.ForPlugin("provider").Register(descriptor, func(ctx context.Context, _ contract.Request) (contract.Response, error) {
		<-ctx.Done()
		return contract.Response{}, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}

	_, err := gateway.ForPlugin("caller").Invoke(context.Background(), contract.Request{Service: descriptor.Name, Method: "Run"})
	var serviceError *contract.ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != contract.ErrorTimeout {
		t.Fatalf("错误 = %v，期望 timeout", err)
	}
}

func newTestGateway(t *testing.T, checker testPolicy, timeout time.Duration) *Gateway {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gateway, err := New(registry.New(), checker, logger, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}
