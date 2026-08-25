package policy

import (
	"strings"
	"testing"
)

func TestStaticDefaultDeny(t *testing.T) {
	checker, err := Decode(strings.NewReader(`
default: deny
plugins: {}
`))
	if err != nil {
		t.Fatal(err)
	}
	if checker.AllowPublish("unknown", "bot.message.group") {
		t.Fatal("未声明插件不应获得发布权限")
	}
	if checker.AllowInvoke("unknown", "protocol.qq.action", "SendMessage") {
		t.Fatal("未声明插件不应获得调用权限")
	}
}

func TestStaticUsesExactMatches(t *testing.T) {
	checker, err := Decode(strings.NewReader(`
default: deny
plugins:
  example.reply:
    publish: [runtime.reply.ready]
    subscribe: [bot.message.group]
    invoke:
      - service: protocol.qq.action
        methods: [SendMessage]
    provide: [reply.generate]
`))
	if err != nil {
		t.Fatal(err)
	}

	if !checker.AllowSubscribe("example.reply", "bot.message.group") {
		t.Fatal("声明的订阅应被允许")
	}
	if checker.AllowSubscribe("example.reply", "bot.message.private") {
		t.Fatal("未声明的相似 Topic 不应被允许")
	}
	if !checker.AllowInvoke("example.reply", "protocol.qq.action", "SendMessage") {
		t.Fatal("声明的服务方法应被允许")
	}
	if checker.AllowInvoke("example.reply", "protocol.qq.action", "DeleteMessage") {
		t.Fatal("未声明的方法不应被允许")
	}
}

func TestStaticDefaultAllowAppliesToDeclaredPlugin(t *testing.T) {
	checker, err := Decode(strings.NewReader(`
default: allow
plugins:
  example:
    publish: []
    subscribe: []
    invoke: []
    provide: []
`))
	if err != nil {
		t.Fatal(err)
	}
	if !checker.AllowPublish("example", "any.topic") || !checker.AllowInvoke("example", "any.service", "Run") {
		t.Fatal("default allow 应适用于已声明插件未列出的能力")
	}
}

func TestStaticRejectsUnknownField(t *testing.T) {
	_, err := Decode(strings.NewReader(`
default: deny
unknown: true
plugins: {}
`))
	if err == nil {
		t.Fatal("未知字段应被拒绝")
	}
}
