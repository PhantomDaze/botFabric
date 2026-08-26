// Package wsconn 封装 gorilla/websocket 连接的写锁、关闭、心跳与 deadline 治理。
// onebotapp 与 protocol/onebot 两个适配器复用本包，消除重复的连接管理代码。
package wsconn

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Options 控制连接助手的行为。
type Options struct {
	// HeartbeatInterval 是 Ping 发送间隔；0 表示不启用心跳与读 deadline 续期。
	HeartbeatInterval time.Duration
	// WriteTimeout 是数据帧写超时；0 表示不加写 deadline（保持兼容）。
	WriteTimeout time.Duration
	// MaxMessageSize 是读消息上限字节；0 表示不限制。
	MaxMessageSize int64
}

// Conn 封装 gorilla/websocket 连接的写锁、关闭、心跳与 deadline 治理。
// 它本身不读循环，只提供心跳 goroutine 与写/关闭原语；
// 读循环由调用方在单独 goroutine 中调用 ReadMessage 驱动。
type Conn struct {
	socket        *websocket.Conn
	writeMu       sync.Mutex
	closeOnce     sync.Once
	heartbeat     time.Duration
	readWindow    time.Duration
	writeTimeout  time.Duration
	maxSize       int64
	heartbeatMu   sync.Mutex
	heartbeatStop chan struct{}
	runWG         sync.WaitGroup
}

// New 创建连接助手。socket 必须非空。
func New(socket *websocket.Conn, opts Options) *Conn {
	c := &Conn{
		socket:       socket,
		heartbeat:    opts.HeartbeatInterval,
		readWindow:   opts.HeartbeatInterval * 2,
		writeTimeout: opts.WriteTimeout,
		maxSize:      opts.MaxMessageSize,
	}
	if c.heartbeat < 0 {
		c.heartbeat = 0
	}
	if c.readWindow < 0 {
		c.readWindow = 0
	}
	if c.writeTimeout < 0 {
		c.writeTimeout = 0
	}
	if c.maxSize > 0 {
		socket.SetReadLimit(c.maxSize)
	}
	return c
}

// StartKeepalive 启动心跳 goroutine：周期性发送 PingMessage 控制帧，
// 并在每次写后保持读 deadline 为 heartbeat*2。需要先调用一次以设置初始读 deadline。
// 若 HeartbeatInterval 为 0 则不启用（保持兼容的裸读行为）。
// 返回的 stop 函数停止心跳 ticker（幂等）。
func (c *Conn) StartKeepalive() (stop func()) {
	if c.heartbeat <= 0 {
		return func() {}
	}
	c.socket.SetPongHandler(func(string) error {
		if c.readWindow > 0 {
			_ = c.socket.SetReadDeadline(time.Now().Add(c.readWindow))
		}
		return nil
	})
	if c.readWindow > 0 {
		_ = c.socket.SetReadDeadline(time.Now().Add(c.readWindow))
	}
	stopCh := make(chan struct{})
	c.heartbeatMu.Lock()
	c.heartbeatStop = stopCh
	c.heartbeatMu.Unlock()
	go c.heartbeatLoop(stopCh)
	return func() {
		c.heartbeatMu.Lock()
		if c.heartbeatStop != nil {
			close(c.heartbeatStop)
			c.heartbeatStop = nil
		}
		c.heartbeatMu.Unlock()
	}
}

func (c *Conn) heartbeatLoop(stopCh chan struct{}) {
	ticker := time.NewTicker(c.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			err := c.socket.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// TrackRun 在调用方启动 run goroutine 前调用，配合 WaitRun 实现 Shutdown 确定性。
func (c *Conn) TrackRun() {
	c.runWG.Add(1)
}

// RunDone 在 run goroutine 退出时调用。
func (c *Conn) RunDone() {
	c.runWG.Done()
}

// WaitRun 等待所有 run goroutine 退出；Shutdown/Stop 应在 Close 后调用。
func (c *Conn) WaitRun() {
	c.runWG.Wait()
}

// WriteJSON 序列化并以 TextMessage 写出，带 writeTimeout 写 deadline。
func (c *Conn) WriteJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteMessage(websocket.TextMessage, payload)
}

// WriteMessage 写一条消息，带 writeTimeout 写 deadline。
func (c *Conn) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeTimeout > 0 {
		_ = c.socket.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	return c.socket.WriteMessage(messageType, data)
}

// WriteControl 写控制帧，固定 1 秒写 deadline（Ping/Pong/Close 帧）。
func (c *Conn) WriteControl(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.socket.WriteControl(messageType, data, time.Now().Add(time.Second))
}

// ReadMessage 透传读消息。调用方在 run goroutine 中循环调用。
func (c *Conn) ReadMessage() (int, []byte, error) {
	return c.socket.ReadMessage()
}

// Close 幂等关闭连接：发送 CloseMessage 控制帧并关闭底层 socket。
func (c *Conn) Close(code int, text string) error {
	var err error
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		_ = c.socket.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
		err = c.socket.Close()
	})
	return err
}
