package client

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

var errClientClosed = errors.New("client is closed")

// gate 是可取消等待的 FIFO 读写门禁：读共享、写独占，等待者按到达顺序获得（写者不会被持续到达的读者饿死）。
// 零值可用。等待阶段不创建额外 goroutine：每个等待者只持有一个 channel，
// 由释放方在持有 mu 时授予，ctx 取消或 Close 时从队列移除/唤醒。
// 所有状态由单一互斥锁 mu 保护，mu 仅在内存状态变更期间持有，绝不跨 RPC，因此不存在锁顺序问题。
type gate struct {
	mu      sync.Mutex
	readers int  // 当前持有读门禁的数量
	writer  bool // 是否有写者持有
	closing bool // Close 已开始：拒绝新请求
	queue   []*gateWaiter
	idle    chan struct{} // Close 等待在途操作排空时创建
	done    chan struct{} // Close 完成后关闭（供重复 Close 等待）
}

type gateWaiter struct {
	write   bool
	granted bool
	ch      chan error // 容量 1：nil 表示已获得，非 nil 表示被 Close 拒绝
}

// acquire 获取读(write=false)或写(write=true)门禁。ctx 先于等待检查，已取消时不占用门禁。
func (g *gate) acquire(ctx context.Context, write bool) error {
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		return errClientClosed
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	if len(g.queue) == 0 && g.canGrant(write) {
		g.grant(write)
		g.mu.Unlock()
		return nil
	}
	w := &gateWaiter{write: write, ch: make(chan error, 1)}
	g.queue = append(g.queue, w)
	g.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
	}

	g.mu.Lock()
	if w.granted {
		// 取消与授予竞争且授予先发生：已持有门禁，归还后报告取消，不发 RPC
		g.mu.Unlock()
		g.release(write)
		return ctx.Err()
	}
	for i, q := range g.queue {
		if q == w {
			g.queue = append(g.queue[:i], g.queue[i+1:]...)
			break
		}
	}
	// 队首等待者离开可能让后面的等待者可以获得
	g.dispatch()
	g.mu.Unlock()
	return ctx.Err()
}

// release 归还 acquire 成功获得的门禁
func (g *gate) release(write bool) {
	g.mu.Lock()
	if write {
		g.writer = false
	} else {
		g.readers--
	}
	g.dispatch()
	if g.closing && g.idle != nil && g.readers == 0 && !g.writer {
		close(g.idle)
		g.idle = nil
	}
	g.mu.Unlock()
}

func (g *gate) canGrant(write bool) bool {
	if write {
		return g.readers == 0 && !g.writer
	}
	return !g.writer
}

func (g *gate) grant(write bool) {
	if write {
		g.writer = true
	} else {
		g.readers++
	}
}

// dispatch 严格按队列顺序授予，调用方须持有 mu
func (g *gate) dispatch() {
	for len(g.queue) > 0 {
		w := g.queue[0]
		if !g.canGrant(w.write) {
			return
		}
		g.queue = g.queue[1:]
		g.grant(w.write)
		w.granted = true
		w.ch <- nil
	}
}

// isClosing 返回 Close 是否已开始
func (g *gate) isClosing() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closing
}

// close 标记 closing（拒绝新请求并唤醒所有等待者返回 closed），等待在途操作结束后执行 fn（仅一次）。
// 并发的重复调用等待首个调用完成后返回 nil。
func (g *gate) close(fn func() error) error {
	g.mu.Lock()
	if g.closing {
		if g.done == nil {
			g.done = make(chan struct{})
		}
		done := g.done
		g.mu.Unlock()
		<-done
		return nil
	}
	g.closing = true
	g.done = make(chan struct{})
	for _, w := range g.queue {
		w.ch <- errClientClosed
	}
	g.queue = nil
	var idle chan struct{}
	if g.readers > 0 || g.writer {
		g.idle = make(chan struct{})
		idle = g.idle
	}
	g.mu.Unlock()

	if idle != nil {
		<-idle
	}
	// fn panic 时也要通知重复的 Close，避免它们永久阻塞（panic 本身继续向上传播）
	defer func() {
		g.mu.Lock()
		close(g.done)
		g.mu.Unlock()
	}()
	return fn()
}
