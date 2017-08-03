package module

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/monitoring"
	"github.com/elastic/beats/libbeat/publisher/beat"
	"github.com/elastic/beats/libbeat/testing"
	"github.com/elastic/beats/metricbeat/mb"

	"github.com/mitchellh/hashstructure"
)

// Expvar metric names.
const (
	successesKey = "success"
	failuresKey  = "failures"
	eventsKey    = "events"
)

var (
	debugf = logp.MakeDebug("module")

	fetchesLock = sync.Mutex{}
	fetches     = map[string]*stats{}
)

// Wrapper contains the Module and the private data associated with
// running the Module and its MetricSets.
//
// Use NewWrapper or NewWrappers to construct new Wrappers.
type Wrapper struct {
	mb.Module
	metricSets    []*metricSetWrapper // List of pointers to its associated MetricSets.
	configHash    uint64
	maxStartDelay time.Duration
}

// metricSetWrapper contains the MetricSet and the private data associated with
// running the MetricSet. It contains a pointer to the parent Module.
type metricSetWrapper struct {
	mb.MetricSet
	module *Wrapper // Parent Module.
	stats  *stats   // stats for this MetricSet.
}

// stats bundles common metricset stats.
type stats struct {
	key      string          // full stats key
	ref      uint32          // number of modules/metricsets reusing stats instance
	success  *monitoring.Int // Total success events.
	failures *monitoring.Int // Total error events.
	events   *monitoring.Int // Total events published.
}

// NewWrapper create a new Module and its associated MetricSets based
// on the given configuration.
func NewWrapper(maxStartDelay time.Duration, config *common.Config, r *mb.Register) (*Wrapper, error) {
	module, metricsets, err := mb.NewModule(config, r)
	if err != nil {
		return nil, err
	}

	wrapper := &Wrapper{
		Module:        module,
		maxStartDelay: maxStartDelay,
		metricSets:    make([]*metricSetWrapper, len(metricsets)),
	}

	for i, ms := range metricsets {
		wrapper.metricSets[i] = &metricSetWrapper{
			MetricSet: ms,
			module:    wrapper,
			stats:     getMetricSetStats(wrapper.Name(), ms.Name()),
		}
	}

	return wrapper, nil
}

// Wrapper methods

// Start starts the Module's MetricSet workers which are responsible for
// fetching metrics. The workers will continue to periodically fetch until the
// done channel is closed. When the done channel is closed all MetricSet workers
// will stop and the returned output channel will be closed.
//
// The returned channel is buffered with a length one one. It must drained to
// prevent blocking the operation of the MetricSets.
//
// Start should be called only once in the life of a Wrapper.
func (mw *Wrapper) Start(done <-chan struct{}) <-chan beat.Event {
	debugf("Starting %s", mw)

	out := make(chan beat.Event, 1)

	// Start one worker per MetricSet + host combination.
	var wg sync.WaitGroup
	wg.Add(len(mw.metricSets))
	for _, msw := range mw.metricSets {
		go func(msw *metricSetWrapper) {
			defer releaseStats(msw.stats)
			defer wg.Done()
			defer msw.close()
			msw.run(done, out)
		}(msw)
	}

	// Close the output channel when all writers to the channel have stopped.
	go func() {
		wg.Wait()
		close(out)
		debugf("Stopped %s", mw)
	}()

	return out
}

// String returns a string representation of Wrapper.
func (mw *Wrapper) String() string {
	return fmt.Sprintf("Wrapper[name=%s, len(metricSetWrappers)=%d]",
		mw.Name(), len(mw.metricSets))
}

// Hash returns the hash value of the module wrapper
// This allows to check if two modules are the same / have the same config
func (mw *Wrapper) Hash() uint64 {
	// Check if hash was calculated previously
	if mw.configHash > 0 {
		return mw.configHash
	}
	var err error

	// Config is unpacked into map[string]interface{} to also take metricset
	// configs into account for the hash.
	var c map[string]interface{}
	mw.UnpackConfig(&c)
	mw.configHash, err = hashstructure.Hash(c, nil)
	if err != nil {
		logp.Err("Error creating config hash for module %s: %s", mw.String(), err)
	}
	return mw.configHash
}

// MetricSets return the list of metricsets of the module
func (mw *Wrapper) MetricSets() []*metricSetWrapper {
	return mw.metricSets
}

// metricSetWrapper methods

func (msw *metricSetWrapper) run(done <-chan struct{}, out chan<- beat.Event) {
	defer logp.Recover(fmt.Sprintf("recovered from panic while fetching "+
		"'%s/%s' for host '%s'", msw.module.Name(), msw.Name(), msw.Host()))

	// Start each metricset randomly over a period of MaxDelayPeriod.
	if msw.module.maxStartDelay > 0 {
		delay := time.Duration(rand.Int63n(int64(msw.module.maxStartDelay)))
		debugf("%v/%v will start after %v", msw.module.Name(), msw.Name(), delay)
		select {
		case <-done:
			return
		case <-time.After(delay):
		}
	}

	debugf("Starting %s", msw)
	defer debugf("Stopped %s", msw)

	// Events and errors are reported through this.
	reporter := &eventReporter{
		msw:  msw,
		out:  out,
		done: done,
	}

	switch ms := msw.MetricSet.(type) {
	case mb.PushMetricSet:
		ms.Run(reporter)
	case mb.EventFetcher, mb.EventsFetcher, mb.ReportingMetricSet:
		msw.startPeriodicFetching(reporter)
	default:
		// Earlier startup stages prevent this from happening.
		logp.Err("MetricSet '%s/%s' does not implement an event producing interface",
			msw.Module().Name(), msw.Name())
	}
}

// startPeriodicFetching performs an immediate fetch for the MetricSet then it
// begins a continuous timer scheduled loop to fetch data. To stop the loop the
// done channel should be closed.
func (msw *metricSetWrapper) startPeriodicFetching(reporter reporter) {
	// Fetch immediately.
	msw.fetch(reporter)

	// Start timer for future fetches.
	t := time.NewTicker(msw.Module().Config().Period)
	defer t.Stop()
	for {
		select {
		case <-reporter.Done():
			return
		case <-t.C:
			msw.fetch(reporter)
		}
	}
}

// fetch invokes the appropriate Fetch method for the MetricSet and publishes
// the result using the publisher client. This method will recover from panics
// and log a stack track if one occurs.
func (msw *metricSetWrapper) fetch(reporter reporter) {
	switch fetcher := msw.MetricSet.(type) {
	case mb.EventFetcher:
		msw.singleEventFetch(fetcher, reporter)
	case mb.EventsFetcher:
		msw.multiEventFetch(fetcher, reporter)
	case mb.ReportingMetricSet:
		msw.reportingFetch(fetcher, reporter)
	default:
		panic(fmt.Sprintf("unexpected fetcher type for %v", msw))
	}
}

func (msw *metricSetWrapper) singleEventFetch(fetcher mb.EventFetcher, reporter reporter) {
	reporter.StartFetchTimer()
	event, err := fetcher.Fetch()
	reporter.ErrorWith(err, event)
}

func (msw *metricSetWrapper) multiEventFetch(fetcher mb.EventsFetcher, reporter reporter) {
	reporter.StartFetchTimer()
	events, err := fetcher.Fetch()
	if len(events) == 0 {
		reporter.ErrorWith(err, nil)
	} else {
		for _, event := range events {
			reporter.ErrorWith(err, event)
		}
	}
}

func (msw *metricSetWrapper) reportingFetch(fetcher mb.ReportingMetricSet, reporter reporter) {
	reporter.StartFetchTimer()
	fetcher.Fetch(reporter)
}

// close closes the underlying MetricSet if it implements the mb.Closer
// interface.
func (msw *metricSetWrapper) close() error {
	if closer, ok := msw.MetricSet.(mb.Closer); ok {
		return closer.Close()
	}
	return nil
}

// String returns a string representation of metricSetWrapper.
func (msw *metricSetWrapper) String() string {
	return fmt.Sprintf("metricSetWrapper[module=%s, name=%s, host=%s]",
		msw.module.Name(), msw.Name(), msw.Host())
}

func (msw *metricSetWrapper) Test(d testing.Driver) {
	done := make(chan struct{})
	d.Run(msw.Name(), func(d testing.Driver) {
		// ReportingMetricSet would hang out forever, perhaps we can add a timeout based test in the future
		if _, ok := msw.MetricSet.(mb.ReportingMetricSet); ok {
			d.Warn("test", "metricset doesn't support testing")
			return
		}

		reporter := &testingReporter{
			driver: d,
			done:   done,
		}
		msw.fetch(reporter)
	})
}

// Reporter implementation

type reporter interface {
	mb.PushReporter
	StartFetchTimer()
}

// eventReporter implements the Reporter interface which is a callback interface
// used by MetricSet implementations to report an event(s), an error, or an error
// with some additional metadata.
type eventReporter struct {
	msw   *metricSetWrapper
	done  <-chan struct{}
	out   chan<- beat.Event
	start time.Time // Start time of the current fetch (or zero for push sources).
}

// startFetchTimer demarcates the start of a new fetch. The elapsed time of a
// fetch is computed based on the time of this call.
func (r *eventReporter) StartFetchTimer() {
	r.start = time.Now()
}

func (r *eventReporter) Done() <-chan struct{} {
	return r.done
}

func (r *eventReporter) Event(event common.MapStr) bool {
	return r.ErrorWith(nil, event)
}

func (r *eventReporter) Error(err error) bool {
	return r.ErrorWith(err, nil)
}

func (r *eventReporter) ErrorWith(err error, meta common.MapStr) bool {
	// Skip nil events without error
	if err == nil && meta == nil {
		return true
	}
	timestamp := r.start
	elapsed := time.Duration(0)

	if !timestamp.IsZero() {
		elapsed = time.Since(timestamp)
	} else {
		timestamp = time.Now()
	}

	if err == nil {
		r.msw.stats.success.Add(1)
	} else {
		r.msw.stats.failures.Add(1)
	}

	event, err := createEvent(r.msw, meta, err, timestamp, elapsed)
	if err != nil {
		logp.Err("createEvent failed: %v", err)
		return false
	}

	if !writeEvent(r.done, r.out, event) {
		return false
	}
	r.msw.stats.events.Add(1)

	return true
}

// other utility functions

func writeEvent(done <-chan struct{}, out chan<- beat.Event, event beat.Event) bool {
	select {
	case <-done:
		return false
	case out <- event:
		return true
	}
}

func getMetricSetStats(module, name string) *stats {
	key := fmt.Sprintf("metricbeat.%s.%s", module, name)

	fetchesLock.Lock()
	defer fetchesLock.Unlock()

	if s := fetches[key]; s != nil {
		s.ref++
		return s
	}

	reg := monitoring.Default.NewRegistry(key)
	s := &stats{
		key:      key,
		ref:      1,
		success:  monitoring.NewInt(reg, successesKey),
		failures: monitoring.NewInt(reg, failuresKey),
		events:   monitoring.NewInt(reg, eventsKey),
	}

	fetches[key] = s
	return s
}

func releaseStats(s *stats) {
	fetchesLock.Lock()
	defer fetchesLock.Unlock()

	s.ref--
	if s.ref > 0 {
		return
	}

	delete(fetches, s.key)
	monitoring.Default.Remove(s.key)
}
