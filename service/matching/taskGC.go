// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/persistence"
)

type taskGC struct {
	lock           int64
	db             *taskQueueDB
	ackLevel       int64
	lastDeleteTime time.Time
	config         *taskQueueConfig
}

var maxTimeBetweenTaskDeletes = time.Second

// newTaskGC returns an instance of a task garbage collector object
// taskGC internally maintains a delete cursor and attempts to delete
// a batch of tasks everytime Run() method is called.
//
// In order for the taskGC to actually delete tasks when Run() is called, one of
// two conditions must be met
//  - Size Threshold: More than MaxDeleteBatchSize tasks are waiting to be deleted (rough estimation)
//  - Time Threshold: Time since previous delete was attempted exceeds maxTimeBetweenTaskDeletes
//
// Finally, the Run() method is safe to be called from multiple threads. The underlying
// implementation will make sure only one caller executes Run() and others simply bail out
func newTaskGC(db *taskQueueDB, config *taskQueueConfig) *taskGC {
	return &taskGC{db: db, config: config}
}

// Run deletes a batch of completed tasks, if its possible to do so
// Only attempts deletion if size or time thresholds are met
func (tgc *taskGC) Run(ctx context.Context, ackLevel int64) {
	tgc.tryDeleteNextBatch(ctx, ackLevel, false)
}

// RunNow deletes a batch of completed tasks if its possible to do so
// This method attempts deletions without waiting for size/time threshold to be met
func (tgc *taskGC) RunNow(ctx context.Context, ackLevel int64) {
	tgc.tryDeleteNextBatch(ctx, ackLevel, true)
}

//tryDeleteNextBatch 尝试删除数据库中已经acked的rows，删除成功就调整tgc.ackLevel
func (tgc *taskGC) tryDeleteNextBatch(ctx context.Context, ackLevel int64, ignoreTimeCond bool) {
	//尝试给taskGC加锁，不成功则返回（代表一个GC routine正在执行）
	if !tgc.tryLock() {
		return
	}
	defer tgc.unlock()
	//检查是否需要清理，
	batchSize := tgc.config.MaxTaskDeleteBatchSize()
	if !tgc.checkPrecond(ackLevel, batchSize, ignoreTimeCond) {
		return
	}
	//进行清理。
	tgc.lastDeleteTime = time.Now().UTC()
	n, err := tgc.db.CompleteTasksLessThan(ctx, ackLevel+1, batchSize)
	if err != nil {
		return
	}
	// implementation behavior for CompleteTasksLessThan:
	// - unit test, cassandra: always return UnknownNumRowsAffected (in this case means "all")
	// - sql: return number of rows affected (should be <= batchSize)
	// if we get UnknownNumRowsAffected or a smaller number than our limit, we know we got
	// everything <= ackLevel, so we can reset ours. if not, we may have to try again.
	if n == persistence.UnknownNumRowsAffected || n < batchSize {
		//重置tgc.ackLevel
		tgc.ackLevel = ackLevel
	}
}

//checkPrecond 检查是否需要清理。
//检查尚未清理的task数据是否够一个backSize,如果够，直接返回true，如果有尚未清理的，那么返回 ignoreTimeCond||到达清理时间间隔
func (tgc *taskGC) checkPrecond(ackLevel int64, batchSize int, ignoreTimeCond bool) bool {
	//检查尚未清理的task数据是否够一个backSize,如果够，直接返回true
	backlog := ackLevel - tgc.ackLevel
	if backlog >= int64(batchSize) {
		return true
	}
	//如果有尚未清理的，那么返回 ignoreTimeCond||到达清理时间间隔
	return backlog > 0 && (ignoreTimeCond || time.Now().UTC().Sub(tgc.lastDeleteTime) > maxTimeBetweenTaskDeletes)
}

func (tgc *taskGC) tryLock() bool {
	return atomic.CompareAndSwapInt64(&tgc.lock, 0, 1)
}

func (tgc *taskGC) unlock() {
	atomic.StoreInt64(&tgc.lock, 0)
}
