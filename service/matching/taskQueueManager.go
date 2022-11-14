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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	uberatomic "go.uber.org/atomic"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
)

const (
	// Time budget for empty task to propagate through the function stack and be returned to
	// pollForActivityTask or pollForWorkflowTask handler.
	returnEmptyTaskTimeBudget = time.Second

	// Fake Task ID to wrap a task for syncmatch
	syncMatchTaskId = -137

	ioTimeout = 5 * time.Second

	// Threshold for counting a AddTask call as a no recent poller call
	noPollerThreshold = time.Minute * 2
)

var (
	// this retry policy is currenly only used for matching persistence operations
	// that, if failed, the entire task queue needs to be reload
	persistenceOperationRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(1 * time.Second).
		WithExpirationInterval(30 * time.Second)
)

type (
	taskQueueManagerOpt func(*taskQueueManagerImpl)

	idBlockAllocator interface {
		RenewLease(context.Context) (taskQueueState, error)
		RangeID() int64
	}

	addTaskParams struct {
		execution     *commonpb.WorkflowExecution
		taskInfo      *persistencespb.TaskInfo
		source        enumsspb.TaskSource
		forwardedFrom string
	}

	taskQueueManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		// AddTask adds a task to the task queue. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		AddTask(ctx context.Context, params addTaskParams) (syncMatch bool, err error)
		// GetTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		GetTask(ctx context.Context, maxDispatchPerSecond *float64) (*internalTask, error)
		// DispatchTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchTask(ctx context.Context, task *internalTask) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// GetVersioningData returns the versioning data for this task queue
		GetVersioningData(ctx context.Context) (*persistencespb.VersioningData, error)
		// MutateVersioningData allows callers to update versioning data for this task queue
		MutateVersioningData(ctx context.Context, mutator func(*persistencespb.VersioningData) error) error
		// InvalidateMetadata allows callers to invalidate cached data on this task queue
		InvalidateMetadata(request *matchingservice.InvalidateTaskQueueMetadataRequest) error
		CancelPoller(pollerID string)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// DescribeTaskQueue returns information about the target task queue
		DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		String() string
		QueueID() *taskQueueID
		TaskQueueKind() enumspb.TaskQueueKind
	}

	// Single task queue in memory state
	taskQueueManagerImpl struct {
		status            int32
		taskQueueID       *taskQueueID
		taskQueueKind     enumspb.TaskQueueKind // sticky taskQueue has different process in persistence
		config            *taskQueueConfig
		db                *taskQueueDB
		taskWriter        *taskWriter
		taskReader        *taskReader // reads tasks from db and async matches it with poller
		liveness          *liveness
		taskGC            *taskGC
		taskAckManager    ackManager   // tracks ackLevel for delivered messages
		matcher           *TaskMatcher // for matching a task producer with a poller
		namespaceRegistry namespace.Registry
		logger            log.Logger
		matchingClient    matchingservice.MatchingServiceClient
		metricsClient     metrics.Client
		namespace         namespace.Name
		metricScope       metrics.Scope // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory *pollerHistory
		// outstandingPollsMap is needed to keep track of all outstanding pollers for a
		// particular taskqueue.  PollerID generated by frontend is used as the key and
		// CancelFunc is the value.  This is used to cancel the context to unblock any
		// outstanding poller when the frontend detects client connection is closed to
		// prevent tasks being dispatched to zombie pollers.
		outstandingPollsLock sync.Mutex
		outstandingPollsMap  map[string]context.CancelFunc
		signalFatalProblem   func(taskQueueManager)
		clusterMeta          cluster.Metadata
		initializedError     *future.FutureImpl[struct{}]
		// metadataInitialFetch is fulfilled once versioning data is fetched from the root partition. If this TQ is
		// the root partition, it is fulfilled as soon as it is fetched from db.
		metadataInitialFetch *future.FutureImpl[struct{}]
		metadataPoller       metadataPoller
	}

	metadataPoller struct {
		// Ensures that we launch the goroutine for polling for updates only one time
		running           *uberatomic.Bool
		pollIntervalCfgFn dynamicconfig.DurationPropertyFn
		stopChan          chan struct{}
		tqMgr             *taskQueueManagerImpl
	}
)

var _ taskQueueManager = (*taskQueueManagerImpl)(nil)

var errRemoteSyncMatchFailed = serviceerror.NewCanceled("remote sync match failed")

func withIDBlockAllocator(ibl idBlockAllocator) taskQueueManagerOpt {
	return func(tqm *taskQueueManagerImpl) {
		tqm.taskWriter.idAlloc = ibl
	}
}

func newTaskQueueManager(
	e *matchingEngineImpl,
	taskQueue *taskQueueID,
	taskQueueKind enumspb.TaskQueueKind,
	config *Config,
	clusterMeta cluster.Metadata,
	opts ...taskQueueManagerOpt,
) (taskQueueManager, error) {
	/**
	[TaskQueueManager]
	根据namespace,taskQueue,
	每个TaskQueueManager都有一个DB。
	*/
	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(taskQueue.namespaceID)
	if err != nil {
		return nil, err
	}
	nsName := namespaceEntry.Name()

	taskQueueConfig := newTaskQueueConfig(taskQueue, config, nsName)

	db := newTaskQueueDB(e.taskManager, taskQueue.namespaceID, taskQueue, taskQueueKind, e.logger)
	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(taskQueue.name),
		tag.WorkflowTaskQueueType(taskQueue.taskType),
		tag.WorkflowNamespace(nsName.String()))
	metricsScope := metrics.GetPerTaskQueueScope(
		e.metricsClient.Scope(metrics.MatchingTaskQueueMgrScope),
		nsName.String(),
		taskQueue.name,
		taskQueueKind,
	).Tagged(metrics.TaskQueueTypeTag(taskQueue.taskType))
	tlMgr := &taskQueueManagerImpl{
		status:            common.DaemonStatusInitialized,
		namespaceRegistry: e.namespaceRegistry,
		matchingClient:    e.matchingClient,
		metricsClient:     e.metricsClient,
		taskQueueID:       taskQueue,
		taskQueueKind:     taskQueueKind,
		logger:            logger,
		db:                db,
		taskAckManager:    newAckManager(e.logger),
		taskGC:            newTaskGC(db, taskQueueConfig),
		config:            taskQueueConfig,
		//其实就是获取poller的信息的Cache，给每个poller一个identity。而poller信息只有一个ratePerSecond
		pollerHistory:        newPollerHistory(),
		outstandingPollsMap:  make(map[string]context.CancelFunc),
		signalFatalProblem:   e.unloadTaskQueue,
		clusterMeta:          clusterMeta,
		namespace:            nsName,
		metricScope:          metricsScope,
		initializedError:     future.NewFuture[struct{}](),
		metadataInitialFetch: future.NewFuture[struct{}](),
		metadataPoller: metadataPoller{
			running:           uberatomic.NewBool(false),
			pollIntervalCfgFn: e.config.MetadataPollFrequency,
			stopChan:          make(chan struct{}),
		},
	}
	tlMgr.metadataPoller.tqMgr = tlMgr

	tlMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		taskQueueConfig.IdleTaskqueueCheckInterval(),
		func() { tlMgr.signalFatalProblem(tlMgr) },
	)
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.taskReader = newTaskReader(tlMgr)

	var fwdr *Forwarder
	if tlMgr.isFowardingAllowed(taskQueue, taskQueueKind) {
		fwdr = newForwarder(&taskQueueConfig.forwarderConfig, taskQueue, taskQueueKind, e.matchingClient)
	}
	tlMgr.matcher = newTaskMatcher(taskQueueConfig, fwdr, tlMgr.metricScope)
	for _, opt := range opts {
		opt(tlMgr)
	}
	return tlMgr, nil
}

// signalIfFatal calls signalFatalProblem on this taskQueueManagerImpl instance
// if and only if the supplied error represents a fatal condition, e.g. the
// existence of another taskQueueManager newer lease. Returns true if the signal
// is emitted, false otherwise.
func (c *taskQueueManagerImpl) signalIfFatal(err error) bool {
	if err == nil {
		return false
	}
	var condfail *persistence.ConditionFailedError
	if errors.As(err, &condfail) {
		c.metricScope.IncCounter(metrics.ConditionFailedErrorPerTaskQueueCounter)
		c.signalFatalProblem(c)
		return true
	}
	return false
}

func (c *taskQueueManagerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	c.liveness.Start()
	c.taskWriter.Start()
	c.taskReader.Start()
	go c.fetchMetadataFromRootPartitionOnInit(context.TODO())
	c.logger.Info("", tag.LifeCycleStarted)
	c.metricScope.IncCounter(metrics.TaskQueueStartedCounter)
}

func (c *taskQueueManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	// ackLevel in taskAckManager is initialized to -1 and then set to a real value (>= 0) once
	// we've successfully acquired a lease. If it's still -1, then we don't have current
	// metadata. UpdateState would fail on the lease check, but don't even bother calling it.
	ackLevel := c.taskAckManager.getAckLevel()
	if ackLevel >= 0 {
		ctx, cancel := c.newIOContext()
		defer cancel()

		c.db.UpdateState(ctx, ackLevel)
		c.taskGC.RunNow(ctx, ackLevel)
	}
	c.metadataPoller.Stop()
	c.liveness.Stop()
	c.taskWriter.Stop()
	c.taskReader.Stop()
	c.logger.Info("", tag.LifeCycleStopped)
	c.metricScope.IncCounter(metrics.TaskQueueStoppedCounter)
}

func (c *taskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	if err != nil {
		return err
	}
	// We don't really care if the initial fetch worked or not, anything that *requires* a bit of metadata should fail
	// that operation if it's never fetched OK. If the initial fetch errored, the metadataPoller will have been started.
	_, _ = c.metadataInitialFetch.Get(ctx)
	return err
}

// AddTask adds a task to the task queue. This method will first attempt a synchronous
// match with a poller. When there are no pollers or if ratelimit is exceeded, task will
// be written to database and later asynchronously matched with a poller
// 创建Task，Task会被添加到Tasks table中
func (c *taskQueueManagerImpl) AddTask(
	ctx context.Context,
	params addTaskParams,
) (bool, error) {
	if params.forwardedFrom == "" {
		// request sent by history service
		c.liveness.markAlive(time.Now())
	}
	//Question: QueueID为什么还有Root
	//猜想，可能是rootWorkflow
	if c.QueueID().IsRoot() && !c.HasPollerAfter(time.Now().Add(-noPollerThreshold)) {
		// Only checks recent pollers in the root partition
		c.metricScope.IncCounter(metrics.NoRecentPollerTasksPerTaskQueueCounter)
	}

	taskInfo := params.taskInfo

	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(namespace.ID(taskInfo.GetNamespaceId()))
	if err != nil {
		return false, err
	}

	clusterInfos := c.clusterMeta.GetAllClusterInfo()
	for _, clusterInfo := range clusterInfos {
		c.logger.Info("Joehanm-AddTask ClusterMeta", tag.WorkflowID(clusterInfo.RPCAddress))
	}
	if namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		syncMatch, err := c.trySyncMatch(ctx, params)
		if syncMatch {
			return syncMatch, err
		}
	}

	if params.forwardedFrom != "" {
		// forwarded from child partition - only do sync match
		// child partition will persist the task when sync match fails
		return false, errRemoteSyncMatchFailed
	}

	_, err = c.taskWriter.appendTask(params.execution, taskInfo)
	c.signalIfFatal(err)
	if err == nil {
		// 添加task成功后会SignalTaskReader
		c.taskReader.Signal()
	}
	return false, err
}

// GetTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
// 1.在c.outstandingPollsMap装入该pollerIDKey的cancel function
// 2.获取identity，将Poller的Identity放入LRU的Cache
// 3.设置matcher的RateLimit
// 4.从Matcher中读出一个任务，装入（namespace，backlogCount）信息后返回此task
func (c *taskQueueManagerImpl) GetTask(
	ctx context.Context,
	maxDispatchPerSecond *float64,
) (*internalTask, error) {
	//[Question] liveness是用来做什么的
	c.liveness.markAlive(time.Now())

	// We need to set a shorter timeout than the original ctx; otherwise, by the time ctx deadline is
	// reached, instead of emptyTask, context timeout error is returned to the frontend by the rpc stack,
	// which counts against our SLO. By shortening the timeout by a very small amount, the emptyTask can be
	// returned to the handler before a context timeout error is generated.
	childCtx, cancel := c.newChildContext(ctx, c.config.LongPollExpirationInterval(), returnEmptyTaskTimeBudget)
	defer cancel()

	// 作用见下方注释
	pollerID, ok := ctx.Value(pollerIDKey).(string)
	if ok && pollerID != "" {
		// Found pollerID on context, add it to the map to allow it to be canceled in
		// response to CancelPoller call
		// c.outstandingPollsLock 如字面意义所示
		c.outstandingPollsLock.Lock()
		c.outstandingPollsMap[pollerID] = cancel
		c.outstandingPollsLock.Unlock()
		defer func() {
			c.outstandingPollsLock.Lock()
			delete(c.outstandingPollsMap, pollerID)
			c.outstandingPollsLock.Unlock()
		}()
	}

	//从ctx里获取identityKey，如果有，就updatePollerInfo,将PollerInfo放入LRU的Cache
	identity, ok := ctx.Value(identityKey).(string)
	if ok && identity != "" {
		c.pollerHistory.updatePollerInfo(pollerIdentity(identity), maxDispatchPerSecond)
		defer func() {
			// to update timestamp when long poll ends
			c.pollerHistory.updatePollerInfo(pollerIdentity(identity), maxDispatchPerSecond)
		}()
	}

	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(c.taskQueueID.namespaceID)
	if err != nil {
		return nil, err
	}

	// the desired global rate limit for the task queue comes from the
	// poller, which lives inside the client side worker. There is
	// one rateLimiter for this entire task queue and as we get polls,
	// we update the ratelimiter rps if it has changed from the last
	// value. Last poller wins if different pollers provide different values
	// 每个Worker里面有个maxDispatchPerSecond,如果两个Worker同时Poll，那么lastWriteWins
	c.matcher.UpdateRatelimit(maxDispatchPerSecond)

	//[Question] namespaceEntry.ActiveInCluster是什么意思
	if !namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		return c.matcher.PollForQuery(childCtx)
	}

	//尝试从macher的taskC,QueryTaskC中Poll出一个任务来
	task, err := c.matcher.Poll(childCtx)
	if err != nil {
		return nil, err
	}
	//给poll出的task【namespace,backlogCountHint】信息
	task.namespace = c.namespace
	task.backlogCountHint = c.taskAckManager.getBacklogCountHint()
	return task, nil
}

// DispatchTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
// 底层调用了MustOffer
// MustOffer blocks until a consumer is found to handle this task Returns error
// only when context is canceled or the ratelimit is set to zero (allow nothing)
// The passed in context MUST NOT have a deadline associated with it
// 这个offer操作是没有timeLimit的，只有context取消，或者rateLimi置为0时，才会返回，否则会一直等
func (c *taskQueueManagerImpl) DispatchTask(
	ctx context.Context,
	task *internalTask,
) error {
	return c.matcher.MustOffer(ctx, task)
}

// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
// if dispatched to local poller then nil and nil is returned.
// 先根据taskID和request信息获取一个task，调用matcher的OfferQuery
// Query也是要么直接给poller，然后就尝试给parent TaskQueue，如果还不成功就一直block
func (c *taskQueueManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	task := newInternalQueryTask(taskID, request)
	return c.matcher.OfferQuery(ctx, task)
}

// GetVersioningData returns the versioning data for the task queue if any. If this task queue is a sub-partition and
// has no cached data, it will explicitly attempt a fetch from the root partition.
// 功能如上面注释所示,就是从task_queue table中读取data信息，decode，然后从中提取version信息
func (c *taskQueueManagerImpl) GetVersioningData(ctx context.Context) (*persistencespb.VersioningData, error) {
	vd, err := c.db.GetVersioningData(ctx)
	if errors.Is(err, errVersioningDataNotPresentOnPartition) {
		// If this is a non-root-partition with no versioning data, this call is indicating we might expect to find
		// some. Since we may not have started the poller, we want to explicitly attempt to fetch now, because
		// it's possible some was added to the root partition, but it failed to invalidate this partition.
		return c.fetchMetadataFromRootPartition(ctx)
	}
	return vd, err
}

//MutateVersioningData 修改root的mutateVersionData,然后通知各个child taskQueue使用metaDataPoller去取
func (c *taskQueueManagerImpl) MutateVersioningData(ctx context.Context, mutator func(*persistencespb.VersioningData) error) error {
	//[conclusion] 只有root taskQueue才有versionData
	newDat, err := c.db.MutateVersioningData(ctx, mutator)
	c.signalIfFatal(err)
	if err != nil {
		return err
	}
	// We will have errored already if this was not the root workflow partition.
	// Now notify partitions that they should fetch changed data from us
	numParts := util.Max(c.config.NumReadPartitions(), c.config.NumWritePartitions())
	//[Acknowledge] WaitGroup其实就是countDownLatch
	wg := &sync.WaitGroup{}
	for i := 0; i < numParts; i++ {
		for _, tqt := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
			if i == 0 && tqt == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
				continue // Root workflow partition owns the data, skip it.
			}
			wg.Add(1)
			go func(i int, tqt enumspb.TaskQueueType) {
				//生成partition的taskQueue
				tq := c.taskQueueID.mkName(i)
				_, err := c.matchingClient.InvalidateTaskQueueMetadata(ctx,
					&matchingservice.InvalidateTaskQueueMetadataRequest{
						NamespaceId:    c.taskQueueID.namespaceID.String(),
						TaskQueue:      tq,
						TaskQueueType:  tqt,
						VersioningData: newDat,
					})
				if err != nil {
					c.logger.Warn("Failed to notify sub-partition of invalidated versioning data",
						tag.WorkflowTaskQueueName(tq), tag.Error(err))
				}
				wg.Done()
			}(i, tqt)
		}
	}
	wg.Wait()
	return nil
}

//InvalidateMetadata 这个跟MutateVersioningData一对的，它的作用修改自己的versionData，然后使用metadataPoller去取metadata
func (c *taskQueueManagerImpl) InvalidateMetadata(request *matchingservice.InvalidateTaskQueueMetadataRequest) error {
	if request.GetVersioningData() != nil {
		if c.taskQueueID.IsRoot() && c.taskQueueID.taskType == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			// Should never happen. Root partitions do not get their versioning data invalidated.
			c.logger.Warn("A root workflow partition was told to invalidate its versioning data, this should not happen")
			return nil
		}
		//直接修改自己的VersioningData为request中的data
		c.db.setVersioningDataForNonRootPartition(request.GetVersioningData())
		c.metadataPoller.StartIfUnstarted()
	}
	return nil
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
// 返回所有的Poller的信息
func (c *taskQueueManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	return c.pollerHistory.getPollerInfo(time.Time{})
}

//HasPollerAfter 如果有OutstandingPoller，那么一定有accessTime符合条件的Poller
// 如果没有如果有OutstandingPoller，那就用getPollerInfo遍历去找
func (c *taskQueueManagerImpl) HasPollerAfter(accessTime time.Time) bool {
	inflightPollerCount := 0
	c.outstandingPollsLock.Lock()
	inflightPollerCount = len(c.outstandingPollsMap)
	c.outstandingPollsLock.Unlock()
	if inflightPollerCount > 0 {
		return true
	}
	recentPollers := c.pollerHistory.getPollerInfo(accessTime)
	return len(recentPollers) > 0
}

//CancelPoller 使用outstandingPollsMap获取其cancel函数，然后运行cancel
func (c *taskQueueManagerImpl) CancelPoller(pollerID string) {
	c.outstandingPollsLock.Lock()
	cancel, ok := c.outstandingPollsMap[pollerID]
	c.outstandingPollsLock.Unlock()

	if ok && cancel != nil {
		cancel()
	}
}

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes and status of taskqueue's ackManager
// (readLevel, ackLevel, backlogCountHint and taskIDBlock).
// 所有的Poller信息和TaskQueueStatus信息（如上面）
func (c *taskQueueManagerImpl) DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	response := &matchingservice.DescribeTaskQueueResponse{Pollers: c.GetAllPollerInfo()}
	if !includeTaskQueueStatus {
		return response
	}

	taskIDBlock := rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)
	response.TaskQueueStatus = &taskqueuepb.TaskQueueStatus{
		ReadLevel:        c.taskAckManager.getReadLevel(),
		AckLevel:         c.taskAckManager.getAckLevel(),
		BacklogCountHint: c.taskAckManager.getBacklogCountHint(),
		RatePerSecond:    c.matcher.Rate(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: taskIDBlock.start,
			EndId:   taskIDBlock.end,
		},
	}

	return response
}

func (c *taskQueueManagerImpl) String() string {
	buf := new(bytes.Buffer)
	if c.taskQueueID.taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		buf.WriteString("Activity")
	} else {
		buf.WriteString("Workflow")
	}
	rangeID := c.db.RangeID()
	_, _ = fmt.Fprintf(buf, " task queue %v\n", c.taskQueueID.name)
	_, _ = fmt.Fprintf(buf, "RangeID=%v\n", rangeID)
	_, _ = fmt.Fprintf(buf, "TaskIDBlock=%+v\n", rangeIDToTaskIDBlock(rangeID, c.config.RangeSize))
	_, _ = fmt.Fprintf(buf, "AckLevel=%v\n", c.taskAckManager.ackLevel)
	_, _ = fmt.Fprintf(buf, "MaxTaskID=%v\n", c.taskAckManager.getReadLevel())

	return buf.String()
}

// completeTask marks a task as processed. Only tasks created by taskReader (i.e. backlog from db) reach
// here. As part of completion:
//   - task is deleted from the database when err is nil
//   - new task is created and current task is deleted when err is not nil
// 作用如上面的阐述
func (c *taskQueueManagerImpl) completeTask(task *persistencespb.AllocatedTaskInfo, err error) {
	if err != nil {
		// failed to start the task.
		// We cannot just remove it from persistence because then it will be lost.
		// We handle this by writing the task back to persistence with a higher taskID.
		// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
		// again the underlying reason for failing to start will be resolved.
		// Note that RecordTaskStarted only fails after retrying for a long time, so a single task will not be
		// re-written to persistence frequently.
		err = executeWithRetry(context.Background(), func(_ context.Context) error {
			wf := &commonpb.WorkflowExecution{WorkflowId: task.Data.GetWorkflowId(), RunId: task.Data.GetRunId()}
			_, err := c.taskWriter.appendTask(wf, task.Data)
			return err
		})

		if err != nil {
			// OK, we also failed to write to persistence.
			// This should only happen in very extreme cases where persistence is completely down.
			// We still can't lose the old task so we just unload the entire task queue
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationStopTaskQueue,
				tag.Error(err),
				tag.WorkflowTaskQueueName(c.taskQueueID.name),
				tag.WorkflowTaskQueueType(c.taskQueueID.taskType))
			c.signalFatalProblem(c)
			return
		}
		// CompleteTask出错了，前面调用了taskWriter把任务写了进去，所以要用Reader再读出来
		c.taskReader.Signal()
	}

	ackLevel := c.taskAckManager.completeTask(task.GetTaskId())

	// TODO: completeTaskFunc and task.finish() should take in a context
	ctx, cancel := c.newIOContext()
	defer cancel()
	c.taskGC.Run(ctx, ackLevel)
}

func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*rangeSize + 1,
		end:   rangeID * rangeSize,
	}
}

// Retry operation on transient error.
func executeWithRetry(
	ctx context.Context,
	operation func(context.Context) error,
) error {
	err := backoff.ThrottleRetryContext(ctx, operation, persistenceOperationRetryPolicy, func(err error) bool {
		if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
			return false
		}
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			return false
		}
		return common.IsPersistenceTransientError(err)
	})
	return err
}

//trySyncMatch 尝试同步向matcher添加task，1s内为完成就超时
func (c *taskQueueManagerImpl) trySyncMatch(ctx context.Context, params addTaskParams) (bool, error) {
	//建立一个1秒的context
	childCtx, cancel := c.newChildContext(ctx, c.config.SyncMatchWaitDuration(), time.Second)

	// Mocking out TaskId for syncmatch as it hasn't been allocated yet
	fakeTaskIdWrapper := &persistencespb.AllocatedTaskInfo{
		Data:   params.taskInfo,
		TaskId: syncMatchTaskId,
	}
	//制作一个新的task,然后通过matcher添加
	task := newInternalTask(fakeTaskIdWrapper, nil, params.source, params.forwardedFrom, true)
	matched, err := c.matcher.Offer(childCtx, task)
	cancel()
	return matched, err
}

// newChildContext creates a child context with desired timeout.
// if tailroom is non-zero, then child context timeout will be
// the minOf(parentCtx.Deadline()-tailroom, timeout). Use this
// method to create child context when childContext cannot use
// all of parent's deadline but instead there is a need to leave
// some time for parent to do some post-work
func (c *taskQueueManagerImpl) newChildContext(
	parent context.Context,
	timeout time.Duration,
	tailroom time.Duration,
) (context.Context, context.CancelFunc) {
	select {
	case <-parent.Done():
		return parent, func() {}
	default:
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return context.WithTimeout(parent, timeout)
	}
	remaining := deadline.Sub(time.Now().UTC()) - tailroom
	if remaining < timeout {
		timeout = time.Duration(util.Max(0, int64(remaining)))
	}
	return context.WithTimeout(parent, timeout)
}

func (c *taskQueueManagerImpl) isFowardingAllowed(taskQueue *taskQueueID, kind enumspb.TaskQueueKind) bool {
	return !taskQueue.IsRoot() && kind != enumspb.TASK_QUEUE_KIND_STICKY
}

func (c *taskQueueManagerImpl) QueueID() *taskQueueID {
	return c.taskQueueID
}

func (c *taskQueueManagerImpl) TaskQueueKind() enumspb.TaskQueueKind {
	return c.taskQueueKind
}

func (c *taskQueueManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)

	namespace, _ := c.namespaceRegistry.GetNamespaceName(c.taskQueueID.namespaceID)
	ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(namespace.String()))

	return ctx, cancel
}

func (c *taskQueueManagerImpl) fetchMetadataFromRootPartitionOnInit(ctx context.Context) {
	if c.metadataInitialFetch.Ready() {
		return
	}
	_, err := c.fetchMetadataFromRootPartition(ctx)
	c.metadataInitialFetch.Set(struct{}{}, err)
}

// fetchMetadataFromRootPartition fetches metadata from root partition iff this partition is not a root partition.
// Returns the fetched data, if fetching was necessary and successful.
// 将已有的versionData取出来做一个Hash，然后GetTaskQueueMetadata发送Hash值出去，如果root taskQueue发现hash值一致，就会返回true，
// 否则就会返回versionData,
// root TaskQueue的Hash值；如果不一致，就开启metadataPoller
func (c *taskQueueManagerImpl) fetchMetadataFromRootPartition(ctx context.Context) (*persistencespb.VersioningData, error) {
	// Nothing to do if we are the root partition of a workflow queue, since we should own the data.
	// (for versioning - any later added metadata may need to not abort so early)
	if c.taskQueueID.IsRoot() && c.taskQueueID.taskType == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
		return nil, nil
	}

	curDat, err := c.db.GetVersioningData(ctx)
	if err != nil && !errors.Is(err, errVersioningDataNotPresentOnPartition) {
		return nil, err
	}
	curHash := HashVersioningData(curDat)

	rootTqName := c.taskQueueID.GetRoot()
	if len(curHash) == 0 {
		// if we have no data, make sure we send a sigil value, so it's known we desire versioning data
		curHash = []byte{0}
	}
	res, err := c.matchingClient.GetTaskQueueMetadata(ctx, &matchingservice.GetTaskQueueMetadataRequest{
		NamespaceId:               c.taskQueueID.namespaceID.String(),
		TaskQueue:                 rootTqName,
		WantVersioningDataCurhash: curHash,
	})
	// If the root partition returns nil here, then that means our data matched, and we don't need to update.
	// If it's nil because it never existed, then we'd never have any data.
	// It can't be nil due to removing versions, as that would result in a non-nil container with
	// nil inner fields.
	if !res.GetMatchedReqHash() {
		c.db.setVersioningDataForNonRootPartition(res.GetVersioningData())
	}
	// We want to start the poller as long as the root partition has any kind of data (or fetching hasn't worked)
	if res.GetMatchedReqHash() || res.GetVersioningData() != nil || err != nil {
		c.metadataPoller.StartIfUnstarted()
	}
	if err != nil {
		return nil, err
	}
	return res.GetVersioningData(), nil
}

// StartIfUnstarted starts the poller if it's not already started. The passed in function is called repeatedly
// and if it returns true, the poller will shut down, at which point it may be started again.
// 解释如上
func (mp *metadataPoller) StartIfUnstarted() {
	if mp.running.Load() {
		return
	}
	go mp.pollLoop()
}

//pollLoop 每隔一段时间就fetchMetadataFromRootPartition，如果没有fetch到数据就停止返回，直到
//直到下一次told to invalidate data
func (mp *metadataPoller) pollLoop() {
	mp.running.Store(true)
	defer mp.running.Store(false)
	ticker := time.NewTicker(mp.pollIntervalCfgFn())
	defer ticker.Stop()

	for {
		select {
		case <-mp.stopChan:
			return
		case <-ticker.C:
			// In case the interval has changed
			ticker.Reset(mp.pollIntervalCfgFn())
			dat, err := mp.tqMgr.fetchMetadataFromRootPartition(context.TODO())
			if dat == nil && err == nil {
				// Can stop polling since there is no versioning data. Loop will be restarted if we
				// are told to invalidate the data, or we attempt to fetch it via GetVersioningData.
				return
			}
		}
	}
}

func (mp *metadataPoller) Stop() {
	close(mp.stopChan)
}
