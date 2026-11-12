package management

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestServerAuthenticatesAndRedactsDeadLetters(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	checker, err := policy.Decode(strings.NewReader("default: allow\nplugins: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New()
	bus, err := eventbus.New(time.Second, checker, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := gateway.New(reg, checker, logger, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtime.NewManager(reg, bus, gateway, logger, "test-bot", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sink := &eventbus.MemoryDeadLetterSink{}
	sink.Record(eventbus.DeadLetter{Event: contract.Event{ID: "event-1", Topic: "secret", TraceID: "trace-1", Payload: []byte("secret payload"), Headers: map[string]string{"secret": "value"}}, SubscriberID: "plugin", Error: "failed", FailedAt: time.Now()})

	server, err := New("127.0.0.1:0", "0123456789abcdef", manager, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	base := "http://" + server.Addr()

	response, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无 token 状态码 = %d", response.StatusCode)
	}
	_ = response.Body.Close()

	request, _ := http.NewRequest(http.MethodGet, base+"/v1/dead-letters", nil)
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("正确 token 状态码 = %d", response.StatusCode)
	}
	var entries []map[string]any
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(entries)
	if strings.Contains(string(encoded), "secret payload") || strings.Contains(string(encoded), "value") {
		t.Fatalf("死信响应泄露敏感字段: %s", encoded)
	}
	if !strings.Contains(string(encoded), "event-1") {
		t.Fatalf("死信响应缺少事件 ID: %s", encoded)
	}
}

// TestDeadLetterPaginationIsCapped 验证管理 API 分页读取且单页条数有硬上限。
func TestDeadLetterPaginationIsCapped(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	checker, err := policy.Decode(strings.NewReader("default: allow\nplugins: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New()
	bus, err := eventbus.New(time.Second, checker, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := gateway.New(reg, checker, logger, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtime.NewManager(reg, bus, gw, logger, "test-bot", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sink := &eventbus.MemoryDeadLetterSink{}
	for i := 0; i < 300; i++ {
		sink.Record(eventbus.DeadLetter{Event: contract.Event{ID: fmt.Sprintf("event-%d", i)}})
	}
	server, err := New("127.0.0.1:0", "0123456789abcdef", manager, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	base := "http://" + server.Addr()

	fetch := func(query string) []map[string]any {
		request, _ := http.NewRequest(http.MethodGet, base+"/v1/dead-letters"+query, nil)
		request.Header.Set("Authorization", "Bearer 0123456789abcdef")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("状态码 = %d", response.StatusCode)
		}
		var entries []map[string]any
		if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
			t.Fatal(err)
		}
		return entries
	}

	if entries := fetch("?limit=500"); len(entries) != maxDeadLetterPageSize {
		t.Fatalf("单页条数 = %d，期望上限 %d", len(entries), maxDeadLetterPageSize)
	}
	page := fetch("?limit=2&offset=1")
	if len(page) != 2 || page[0]["event_id"] != "event-1" || page[1]["event_id"] != "event-2" {
		t.Fatalf("分页结果 = %#v", page)
	}
}
