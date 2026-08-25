package observability

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// HealthProvider 提供所有插件的健康状态快照。
type HealthProvider interface {
	Health(ctx context.Context) map[string]contract.HealthStatus
}

// NewHealthHandler 创建可由应用端插件或管理端挂载的健康检查处理器。
func NewHealthHandler(provider HealthProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		statuses := provider.Health(request.Context())
		overallHealthy := true
		for _, status := range statuses {
			if status.State != contract.HealthHealthy {
				overallHealthy = false
				break
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		if !overallHealthy {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(writer).Encode(struct {
			Healthy bool                             `json:"healthy"`
			Plugins map[string]contract.HealthStatus `json:"plugins"`
		}{Healthy: overallHealthy, Plugins: statuses})
	})
}
