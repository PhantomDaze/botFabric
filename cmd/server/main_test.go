package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunWithContextStartsAndStopsRuntime(t *testing.T) {
	originalDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(filepath.Join("..", "..")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalDirectory); err != nil {
			t.Errorf("恢复工作目录: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	completed := make(chan error, 1)
	configPath := "config/runtime.example.yaml"

	go func() {
		completed <- runWithContext(ctx, configPath, func() { close(ready) })
	}()

	select {
	case <-ready:
		cancel()
	case err := <-completed:
		t.Fatalf("运行时在启动前退出: %v", err)
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("运行时未在 2 秒内启动")
	}

	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("运行时停机失败: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("运行时未在 2 秒内停止")
	}
}
