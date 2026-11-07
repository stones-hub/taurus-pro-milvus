package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ReadRecoveryInterval 是只读恢复中每次失败后、重建连接前的等待间隔
	ReadRecoveryInterval = 5 * time.Second
	// ReadRecoveryBudget 是启用恢复时单次 Search/Query 调用的总预算，自调用开始计，
	// 覆盖首次门禁等待与首次查询、恢复等待、重建握手、加载等待与重试查询；调用方 ctx 更短时以 ctx 为准
	ReadRecoveryBudget = 60 * time.Second

	// loadStatePollInterval 是集合处于 Loading 时轮询加载状态的间隔
	loadStatePollInterval = 500 * time.Millisecond
)

// ErrReadRecoveryExhausted 表示启用恢复的调用在总预算内未得到结果（作为 ReadRecoveryError.Last）
var ErrReadRecoveryExhausted = errors.New("read recovery budget exhausted")

// ErrReadRecoveryDatabaseChanged 表示恢复无法保持首次查询所用的库身份：重试只在该库上进行，
// 恢复期间 UseDatabase 切换了当前库，或重建时 SDK 实际解析的库与之不同（如当前库被 UseDatabase 置空而地址 URL 带库），
// 均停止恢复，绝不改查其他库（作为 ReadRecoveryError.Last）
var ErrReadRecoveryDatabaseChanged = errors.New("read recovery cannot keep the original database")

// ReadRecoveryEvent 描述启用只读恢复时一次 Search/Query 调用的最终结果。
// 首次查询直接成功时不产生事件；其余每个最终出口恰好产生一次：首次即不可恢复或调用方 ctx 结束（含首次门禁等待中）
// 而原样返回、首次门禁等待或首次查询被恢复预算/Close 截断、进入恢复后成功或失败。
// 据 Err 区分：nil 为恢复成功；*ReadRecoveryError 为进入恢复后失败或首次被预算/Close 截断；
// 其他为未进入恢复、原样返回的错误，此时 Err 与 FirstErr 为同一值。
// 本库不在其中加入认证信息、向量或过滤表达式，但不对错误与集合名脱敏：FirstErr/Err 是服务端/SDK 原样错误，
// 文本可能含集合、通道、用户名、服务地址等上下文；Collection 是调用方传入的原值，未经校验或转义，
// 可能含租户/用户标识或换行等控制字符。记录日志时默认只记录固定的 Op、错误分类、Attempts、Elapsed，
// 不要输出错误原文，也不要记录 Collection；确需区分集合时由调用方映射为自己定义的安全标识，仅转义不等于脱敏。
// 错误分类用 errors.Is 白名单（如 merr.ErrChannelNotAvailable、ErrReadRecoveryExhausted、context.Canceled）：
// status.Code 只反映 gRPC 状态，Milvus 业务错误（如 merr 1/2/503/505）多为 Unknown，无法据此精确分类。
// Attempts 为越过门禁（及库校验）后实际执行查询的次数（含首次）：0 表示首次门禁等待即失败、未发出查询；
// 计入的执行也不保证请求到达服务端（如 Search 参数在本地构建即失败）；SDK 内部对 Unavailable 的重试不计入。
type ReadRecoveryEvent struct {
	Op         string        // "Search" 或 "Query"
	Collection string        // 调用方传入的集合名原值（未转义，默认不应记入日志，见上）
	FirstErr   error         // 首次尝试（含首次门禁等待）的错误，事件中恒非 nil
	Err        error         // 与返回给调用方的错误为同一值，nil 表示恢复成功
	Attempts   int           // 实际执行查询的次数（含首次），见上
	Elapsed    time.Duration // 自调用开始到产生事件的耗时
}

// ReadRecoveryError 是进入只读恢复后仍失败、或首次门禁等待/首次查询被恢复预算/Close 截断时返回的错误。
// 判定只依据最终错误 Last：Unwrap 只返回 Last，GRPCStatus 也只由 Last 决定，
// 因此 errors.Is/As、status.Code 不会因首次失败（往往是可恢复错误）误判为可重试。
// 首次失败只保留在 First 字段与错误文本中，供诊断。
type ReadRecoveryError struct {
	Op         string
	Collection string
	First      error // 首次失败（仅诊断用，不参与 Unwrap）
	Last       error // 终止原因：不可恢复错误、ctx 错误、ErrReadRecoveryExhausted、ErrReadRecoveryDatabaseChanged 或客户端已关闭
	Attempts   int   // 同 ReadRecoveryEvent.Attempts
	Elapsed    time.Duration
}

func (e *ReadRecoveryError) Error() string {
	return fmt.Sprintf("milvus %s on collection %q failed after read recovery (%d attempts, %s): %v; first error: %v",
		e.Op, e.Collection, e.Attempts, e.Elapsed.Round(time.Millisecond), e.Last, e.First)
}

func (e *ReadRecoveryError) Unwrap() error {
	return e.Last
}

// GRPCStatus 只由 Last 决定 gRPC 状态（不递归到本类型自身）：
// context.DeadlineExceeded 与 ErrReadRecoveryExhausted 为 DeadlineExceeded，context.Canceled 为 Canceled；
// Last 自带 gRPC 状态时沿用其状态码；其余（Milvus 业务错误、切库、客户端已关闭、校验失败等）为 Unknown。
func (e *ReadRecoveryError) GRPCStatus() *status.Status {
	code := codes.Unknown
	switch {
	case errors.Is(e.Last, context.DeadlineExceeded), errors.Is(e.Last, ErrReadRecoveryExhausted):
		code = codes.DeadlineExceeded
	case errors.Is(e.Last, context.Canceled):
		code = codes.Canceled
	default:
		if s, ok := status.FromError(e.Last); ok && s != nil {
			code = s.Code()
		}
	}
	return status.New(code, e.Error())
}

// isCtxErr 判断错误是否由 ctx 取消/超时引起（含 gRPC 层的 Canceled/DeadlineExceeded）
func isCtxErr(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}

// isRecoverableReadErr 仅识别经确认的暂时性故障，按 gRPC 状态码与 Milvus 错误码判断，不做字符串匹配：
//   - gRPC Unavailable：连接中断/拒绝（SDK 内部已对其做过短暂重试）
//   - merr ErrServiceNotReady(1) / ErrServiceUnavailable(2)：服务启动中或暂不可用
//   - merr ErrChannelNotAvailable(503)：重启后查询分发尚未可服务（"channel distribution is not serviceable"）
//   - merr ErrChannelTSafeStalled(505)：容器 stop/start 后通道 tSafe 滞后（"channel tsafe stalled"），重连不保证修复，持续出现时按预算失败
//
// ctx 取消/超时、认证、权限、参数、集合未加载/不存在、限流及其他未列出的错误码（含同为 retryable 的其他码）一律不恢复。
func isRecoverableReadErr(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, merr.ErrServiceNotReady) ||
		errors.Is(err, merr.ErrServiceUnavailable) ||
		errors.Is(err, merr.ErrChannelNotAvailable) ||
		errors.Is(err, merr.ErrChannelTSafeStalled) {
		return true
	}
	return status.Code(err) == codes.Unavailable
}

// recovery 保存只读恢复所需的配置与代际状态
type recovery struct {
	opts     Options // 创建时的完整配置副本（含认证/TLS/拨号），仅用于重建，绝不输出
	observer func(ReadRecoveryEvent)

	interval time.Duration
	budget   time.Duration
	loadPoll time.Duration

	// dial 按配置建立新的 SDK 客户端并返回 SDK 实际解析的库名，测试中可替换
	dial func(ctx context.Context, o *Options) (*milvusclient.Client, string, error)
	// onShare 在调用方开始等待他人发起的重建前调用（ctx 为该调用方的恢复 ctx），仅测试设置，用于确定性同步
	onShare func(ctx context.Context)

	life context.Context // Close 时取消，终止所有进行中的恢复
	stop context.CancelFunc

	mu       sync.Mutex
	gen      uint64       // 当前代际，每次替换底层客户端 +1（替换时另持写门禁）
	building *rebuildCall // 进行中的重建（全局同时至多一个），同库同代并发失败者共享；Close 等待其结束
}

// rebuildCall 表示一次进行中的重建；done 关闭后 err/abandoned 只读
type rebuildCall struct {
	db        string // 发起者固定的库，只有同库等待者共享结果
	done      chan struct{}
	err       error
	abandoned bool // 因发起者自身 ctx 结束而失败：等待者应自行重试，而非共享该错误
}

func newRecovery(o Options) *recovery {
	r := &recovery{
		opts:     o,
		observer: o.ReadRecoveryObserver,
		interval: ReadRecoveryInterval,
		budget:   ReadRecoveryBudget,
		loadPoll: loadStatePollInterval,
		dial:     dialSDK,
	}
	r.life, r.stop = context.WithCancel(context.Background())
	return r
}

// waitRebuild 等待已开始的重建结束。须在 stop 之后调用：此后不会再开始新的重建，
// 而进行中的重建 ctx 已随 Close 取消，握手与门禁等待会及时返回，候选客户端在 done 前已关闭或已安装。
func (r *recovery) waitRebuild() {
	r.mu.Lock()
	call := r.building
	r.mu.Unlock()
	if call != nil {
		<-call.done
	}
}

// readFunc 在读门禁内用给定 SDK 句柄执行一次只读查询
type readFunc func(ctx context.Context, cli *milvusclient.Client) error

// read 执行只读查询。未启用恢复时与原行为一致：用调用方 ctx 单次执行并原样返回。
// 启用时自调用开始即以 min(调用方 ctx, 开始+预算) 且 Close 时取消的 rctx 约束全部阶段（含首次门禁等待与首次查询）。
// 首次错误可恢复时在 rctx 内循环：等待间隔 → 重建（同代共享一次）→ 集合 Loading 时等待其完成 → 重试查询；
// 重试只在首次查询所用的库上进行，重试成功的结果须通过 validate。各阶段不跨等待持有门禁。
// 首次错误不可恢复或调用方 ctx 先结束时原样返回；首次门禁等待或首次查询被恢复预算/Close 截断时返回 *ReadRecoveryError。
// 除首次直接成功外，每个出口都在返回前恰好通知一次 observer。
func (c *client) read(ctx context.Context, op, collection string, partitions []string, fn readFunc, validate func() error) error {
	if c.rec == nil {
		_, _, err := c.readOnce(ctx, fn, nil)
		return err
	}

	start := time.Now()
	r := c.rec
	rctx, cancel := context.WithDeadline(ctx, start.Add(r.budget))
	defer cancel()
	defer context.AfterFunc(r.life, cancel)()

	// 只统计越过门禁（及库校验）后实际执行查询的次数；fn 在 readOnce 内同步执行，无并发访问
	attempts := 0
	run := func(ctx context.Context, cli *milvusclient.Client) error {
		attempts++
		return fn(ctx, cli)
	}

	gen, db, err := c.readOnce(rctx, run, nil)
	if err == nil {
		return nil
	}
	first, wrap := err, true
	switch {
	case isRecoverableReadErr(err):
		for {
			gen, err = c.prepareRetry(rctx, gen, db, collection, partitions)
			if err == nil {
				gen, _, err = c.readOnce(rctx, run, &db)
				if err == nil {
					if err = validate(); err == nil {
						break
					}
					err = errors.Wrap(err, "recovered result failed validation")
				}
			}
			err = c.stopErr(ctx, rctx, err)
			if !isRecoverableReadErr(err) {
				break
			}
		}
	case rctx.Err() != nil && ctx.Err() == nil && isCtxErr(err):
		// 首次门禁等待或首次查询被恢复预算 / Close 截断
		err = c.recoveryStopCause(ctx)
	default:
		// 首次即不可恢复，或调用方自身 ctx 结束：与未启用时一样原样返回，不为观察而包装
		wrap = false
	}

	elapsed := time.Since(start)
	if err != nil && wrap {
		err = &ReadRecoveryError{Op: op, Collection: collection, First: first, Last: err, Attempts: attempts, Elapsed: elapsed}
	}
	if r.observer != nil {
		r.observer(ReadRecoveryEvent{Op: op, Collection: collection, FirstErr: first, Err: err, Attempts: attempts, Elapsed: elapsed})
	}
	return err
}

// readOnce 在读门禁内执行一次查询，并返回本次所用的代际与库名（供失败后定位要替换的代际）。
// pinDB 非 nil 时要求当前库仍为 *pinDB，否则不发查询并返回 ErrReadRecoveryDatabaseChanged。
func (c *client) readOnce(ctx context.Context, fn readFunc, pinDB *string) (uint64, string, error) {
	if err := c.gate.acquire(ctx, false); err != nil {
		return 0, "", err
	}
	defer c.gate.release(false)

	gen, db := c.generation(), c.db
	if pinDB != nil && db != *pinDB {
		return gen, db, ErrReadRecoveryDatabaseChanged
	}
	err := fn(ctx, c.cli)
	return gen, db, err
}

func (c *client) generation() uint64 {
	if c.rec == nil {
		return 0
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	return c.rec.gen
}

// recoveryStopCause 区分恢复终止原因：调用方 ctx、客户端关闭或预算耗尽
func (c *client) recoveryStopCause(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.rec.life.Err() != nil {
		return errClientClosed
	}
	return ErrReadRecoveryExhausted
}

// stopErr 在 rctx 已结束时，只把由 ctx 引起、客户端关闭或仍属可恢复的错误归一为终止原因；
// 恰在到期时出现的其他真实错误（权限、集合未加载、切库、结果校验失败等）原样保留
func (c *client) stopErr(ctx, rctx context.Context, err error) error {
	if rctx.Err() == nil {
		return err
	}
	if isCtxErr(err) || isRecoverableReadErr(err) || errors.Is(err, errClientClosed) {
		return c.recoveryStopCause(ctx)
	}
	return err
}

// prepareRetry 等待间隔、替换失败所在代际的底层客户端，并在集合加载中时等待加载完成。
// 返回最后一次 RPC 所观察到的代际：加载状态查询以可恢复错误失败时，下一轮据此重建实际失败的代际。
func (c *client) prepareRetry(ctx context.Context, gen uint64, db, collection string, partitions []string) (uint64, error) {
	if err := sleepCtx(ctx, c.rec.interval); err != nil {
		return gen, err
	}
	if err := c.refresh(ctx, gen, db); err != nil {
		return gen, err
	}
	return c.awaitLoaded(ctx, gen, db, collection, partitions)
}

// refresh 确保代际 gen 已被替换。全局同时至多一个重建；同库同代的并发调用等待并共享其结果（含切库停止），
// 不同库的等待者不共享；发起者因自身 ctx 结束而失败时，仍需要的等待者接替重建。Close 开始后不再发起重建。
func (c *client) refresh(ctx context.Context, gen uint64, db string) error {
	r := c.rec
	for {
		r.mu.Lock()
		if r.gen != gen {
			r.mu.Unlock()
			return nil
		}
		call := r.building
		if call == nil {
			if r.life.Err() != nil {
				r.mu.Unlock()
				return errClientClosed
			}
			call = &rebuildCall{db: db, done: make(chan struct{})}
			r.building = call
			r.mu.Unlock()

			err := c.checkDB(ctx, db)
			if err == nil {
				err = c.rebuild(ctx, gen, db)
			}

			r.mu.Lock()
			r.building = nil
			call.err, call.abandoned = err, ctx.Err() != nil
			close(call.done)
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()

		if r.onShare != nil {
			r.onShare(ctx)
		}
		select {
		case <-call.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		if call.db == db && call.err != nil && !call.abandoned {
			return call.err
		}
	}
}

// checkDB 在读门禁下确认当前库仍为 db（c.db 只在写门禁下修改），切库后不再为旧库建连
func (c *client) checkDB(ctx context.Context, db string) error {
	if err := c.gate.acquire(ctx, false); err != nil {
		return err
	}
	defer c.gate.release(false)
	if c.db != db {
		return ErrReadRecoveryDatabaseChanged
	}
	return nil
}

// rebuild 在门禁外用原配置（库名取首次查询所用的库）建立新客户端，握手受 ctx 限时；
// SDK 实际解析的库名必须与 db 一致（例如 db 为空而地址 URL 带库时 SDK 会改用 URL 库），否则放弃，绝不静默换库。
// 随后持写门禁替换：此时经本库的旧代在途请求均已结束，可安全关闭旧客户端。
// 期间 UseDatabase 已切库时放弃新客户端、保留调用方的切换，返回 ErrReadRecoveryDatabaseChanged。
// 返回前候选客户端必定已关闭或已安装。
func (c *client) rebuild(ctx context.Context, gen uint64, db string) error {
	o := c.rec.opts
	o.DBName = db
	cli, resolved, err := c.rec.dial(ctx, &o)
	if err != nil {
		return err
	}
	if resolved != db {
		_ = cli.Close(context.Background())
		return ErrReadRecoveryDatabaseChanged
	}
	if err := c.gate.acquire(ctx, true); err != nil {
		_ = cli.Close(context.Background())
		return err
	}
	defer c.gate.release(true)

	if c.db != db {
		_ = cli.Close(context.Background())
		return ErrReadRecoveryDatabaseChanged
	}
	c.rec.mu.Lock()
	current := c.rec.gen == gen
	if current {
		c.rec.gen++
	}
	c.rec.mu.Unlock()
	if !current {
		_ = cli.Close(context.Background())
		return nil
	}

	c.cliMu.Lock()
	old := c.cli
	c.cli = cli
	c.cliMu.Unlock()
	if old != nil {
		_ = old.Close(context.Background())
	}
	return nil
}

// awaitLoaded 仅在集合（或所查分区）处于 Loading 时等待其完成。从不发起 Load/Release，
// 不改变调用方的加载意图或副本配置；NotLoad 等其他状态直接交由随后的查询返回服务端的真实错误。
// 状态查询自身的不可恢复失败（如无 GetLoadState 权限）不阻断恢复，同样交由查询判定；
// 当前库已不是 db 时不查询其他库的状态，返回 ErrReadRecoveryDatabaseChanged。
// 返回最后一次状态查询所用的代际（未发出查询时为 gen）。
func (c *client) awaitLoaded(ctx context.Context, gen uint64, db, collection string, partitions []string) (uint64, error) {
	for {
		state, used, err := c.loadState(ctx, db, collection, partitions)
		if used != nil {
			gen = *used
		}
		if err != nil {
			if ctx.Err() != nil || isRecoverableReadErr(err) || errors.Is(err, ErrReadRecoveryDatabaseChanged) {
				return gen, err
			}
			return gen, nil
		}
		if state != entity.LoadStateLoading {
			return gen, nil
		}
		if err := sleepCtx(ctx, c.rec.loadPoll); err != nil {
			return gen, err
		}
	}
}

// loadState 在读门禁内查询加载状态，并返回本次查询所用的代际（未发出查询时为 nil）
func (c *client) loadState(ctx context.Context, db, collection string, partitions []string) (entity.LoadStateCode, *uint64, error) {
	if err := c.gate.acquire(ctx, false); err != nil {
		return 0, nil, err
	}
	defer c.gate.release(false)

	if c.db != db {
		return 0, nil, ErrReadRecoveryDatabaseChanged
	}
	gen := c.generation()
	state, err := c.cli.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(collection, partitions...))
	return state.State, &gen, err
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// validateSearchResults 校验恢复后的搜索结果：每个查询向量对应一个结果集，且结果集解析无错误
func validateSearchResults(results []milvusclient.ResultSet, nq int) error {
	if len(results) != nq {
		return errors.Errorf("got %d result sets for %d query vectors", len(results), nq)
	}
	for i, rs := range results {
		if rs.Err != nil {
			return errors.Wrapf(rs.Err, "result set %d", i)
		}
	}
	return nil
}

// validateQueryColumns 校验恢复后的查询结果：各输出列行数一致
func validateQueryColumns(columns []column.Column) error {
	for _, col := range columns {
		if col.Len() != columns[0].Len() {
			return errors.Errorf("column %q has %d rows, column %q has %d", col.Name(), col.Len(), columns[0].Name(), columns[0].Len())
		}
	}
	return nil
}
