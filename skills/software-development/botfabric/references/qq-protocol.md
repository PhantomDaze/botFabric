# references/qq-protocol.md

## QQ Protocol Adapter Integration

The `QQAdapter` is the entry point for all QQ bot traffic. It must:

1. Connect to the QQ protocol endpoint (onebot or similar)
2. Parse incoming events (private message, group message, event)
3. Normalize the event into the internal `Event` struct
4. Publish the event to the event bus
5. Wait for plugin handlers to produce a response
6. Return the response to the QQ protocol

## Event Normalization

```go
type QQEvent struct {
    Type     string `json:"type"`
    Detail   interface{} `json:"detail"`
    From     string `json:"from"`
    To       string `json:"to"`
    Raw      string `json:"raw"`
}

func (a *QQAdapter) normalize(event QQEvent) gateway.Event {
    return gateway.Event{
        Type:      a.eventType(event),
        Topic:     a.topicFromEvent(event),
        Payload:   event,
        From:      "qq_adapter",
        Timestamp: time.Now(),
    }
}
```

## Common Pitfalls

- Do not parse raw QQ JSON manually — use the official Go SDK
- Always include `TraceID` in metadata for tracing
- Rate limiting must be enforced at the adapter level

## References

- Official OneBot Go SDK: https://github.com/Mrs4s/MiraiGo
- Tencent Bot API: https://bot.q.qq.com/wiki/