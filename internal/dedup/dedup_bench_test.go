package dedup

import (
	"fmt"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

var benchmarkDedupPrimary bool

func BenchmarkDeduplicatorCheck(b *testing.B) {
	for _, test := range []struct {
		name     string
		strategy contract.DedupStrategy
	}{
		{name: "message-id-miss", strategy: contract.DedupMessageID},
		{name: "fingerprint-miss", strategy: contract.DedupFingerprint},
	} {
		b.Run(test.name, func(b *testing.B) {
			deduplicator, err := New(NewMemoryStore(), contract.DedupSpec{
				Enabled:       true,
				Strategy:      test.strategy,
				Window:        time.Hour,
				DuplicateMode: contract.DuplicateDrop,
			})
			if err != nil {
				b.Fatal(err)
			}
			base := contract.GroupMessage{GroupID: "group-1", UserID: "user-1", Text: "short fixed-length message"}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				message := base
				if test.strategy == contract.DedupMessageID {
					message.MessageID = fmt.Sprintf("message-%08d", i)
				} else {
					message.Text = fmt.Sprintf("short fixed-length message %08d", i)
				}
				payload, err := contract.EncodeJSON(message)
				if err != nil {
					b.Fatal(err)
				}
				event := contract.Event{Topic: "bot.message.group", Payload: payload}
				primary, _, _, err := deduplicator.Check(event)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkDedupPrimary = primary
			}
		})
	}
}
