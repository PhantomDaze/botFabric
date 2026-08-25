package sdk_test

import (
	"context"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	sdk "github.com/PhantomDaze/botfabric/sdk/go"
)

type fakeHost struct{}

func (fakeHost) EventBus() contract.EventBus      { return nil }
func (fakeHost) Gateway() contract.ServiceGateway { return nil }
func (fakeHost) Logger() contract.Logger          { return nil }
func (fakeHost) BotID() string                    { return "test" }

func TestClientEncodesAndDecodesPublicModels(t *testing.T) {
	payload, err := sdk.EncodeJSON(contract.GroupMessage{GroupID: "9223372036854775807", Text: "你好"})
	if err != nil {
		t.Fatal(err)
	}
	var message contract.GroupMessage
	if err := sdk.DecodeJSON(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.GroupID != "9223372036854775807" || message.Text != "你好" {
		t.Fatalf("公共模型 = %#v", message)
	}
}

func TestUnavailableClientReturnsStableError(t *testing.T) {
	client := sdk.New(fakeHost{})
	_, err := client.Invoke(context.Background(), contract.Request{Service: "test", Method: "Run"})
	serviceError, ok := err.(*contract.ServiceError)
	if !ok || serviceError.Code != contract.ErrorUnavailable {
		t.Fatalf("错误 = %v", err)
	}
}
