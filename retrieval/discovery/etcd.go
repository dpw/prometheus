package discovery

import (
	"strings"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

const (
	etcdRetryInterval = 15 * time.Second

	// The key under which the target's address was found
	etcdKeyLabel = model.MetaLabelPrefix + "etcd_key"

	// The key of the directory within which the target was found
	etcdDirectoryKeyLabel = model.MetaLabelPrefix + "etcd_directory_key"
)

// EtcdDiscovery retrieves target information from a Etcd server and
// updates them via watches.
type EtcdDiscovery struct {
	client etcd.Client
	kapi   etcd.KeysAPI
	ctx    context.Context
	key    string

	lock sync.Mutex
	run  *etcdRun
}

type etcdRun struct {
	ed      *EtcdDiscovery
	dirs    etcdDirs
	watcher etcd.Watcher
}

// A map from directory keys to a map from keys to values
type etcdDirs map[string]map[string]string

// NewEtcdDiscovery returns a new EtcdDiscovery for the given config.
func NewEtcdDiscovery(conf *config.EtcdSDConfig) (*EtcdDiscovery, error) {
	c, err := etcd.New(etcd.Config{
		Endpoints: conf.Endpoints,
		Username:  conf.Username,
		Password:  conf.Password,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdDiscovery{
		client: c,
		kapi:   etcd.NewKeysAPI(c),
		ctx:    context.Background(),
		key:    conf.DirectoryKey,
	}, nil
}

func (ed *EtcdDiscovery) getRun() *etcdRun {
	ed.lock.Lock()
	defer ed.lock.Unlock()

	if ed.run == nil {
		ed.run = &etcdRun{ed: ed}
	}

	return ed.run
}

func (ed *EtcdDiscovery) Sources() []string {
	run := ed.getRun()
	run.doInitialQuery(ed.ctx)

	var res []string
	for k := range run.dirs {
		res = append(res, k)
	}

	return res
}

func (ed *EtcdDiscovery) Run(up chan<- config.TargetGroup, done <-chan struct{}) {
	run := ed.getRun()
	ctx, cancel := context.WithCancel(ed.ctx)

	// convert the done signal to context cancellation
	go func() {
		<-done
		cancel()
	}()

	go run.run(up, ctx)
}

func (r *etcdRun) doInitialQuery(ctx context.Context) bool {
	var startIndex uint64
	r.dirs = etcdDirs{}

	resp, err := r.ed.kapi.Get(ctx, r.ed.key,
		&etcd.GetOptions{Recursive: true})
	if err != nil {
		if cerr, ok := err.(etcd.Error); ok && cerr.Code == etcd.ErrorCodeKeyNotFound {
			startIndex = cerr.Index
		} else {
			log.Errorf("Error querying etcd: %s", err)
			return false
		}
	} else {
		startIndex = resp.Index
		node := resp.Node
		if node.Dir {
			r.populateDir(node)
		} else {
			r.dir(parentKey(node.Key))[node.Key] = node.Value
		}
	}

	r.watcher = r.ed.kapi.Watcher(r.ed.key, &etcd.WatcherOptions{
		AfterIndex: startIndex,
		Recursive:  true,
	})

	return true
}

func (r *etcdRun) run(up chan<- config.TargetGroup, ctx context.Context) {
	oldDirs := etcdDirs{}

	for {
		if r.watcher == nil && !r.doInitialQuery(ctx) {
			goto delay
		}

		// Send out the initial updates/updates that
		// occurred across a watch error
		r.doUpdatesTo(oldDirs, up)

		for {
			resp, err := r.watcher.Next(ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}

				log.Errorf("Error watching etcd: %s", err)
				r.watcher = nil
				break
			}

			r.handleChange(resp, up)
		}

	delay:
		t := time.NewTimer(etcdRetryInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}

		oldDirs = r.dirs
	}
}

func (r *etcdRun) populateDir(node *etcd.Node) {
	dir := r.dir(node.Key)
	for _, child := range node.Nodes {
		if child.Dir {
			r.populateDir(child)
		} else {
			dir[child.Key] = child.Value
		}
	}
}

func (r *etcdRun) dir(key string) map[string]string {
	dir := r.dirs[key]
	if dir == nil {
		dir = map[string]string{}
		r.dirs[key] = dir
	}

	return dir
}

func parentKey(key string) string {
	slash := strings.LastIndex(key, "/")
	if slash > 1 {
		return key[:slash]
	} else {
		return "/"
	}
}

var etcdDeleteActions = map[string]struct{}{
	"delete":           {},
	"compareAndDelete": {},
	"expire":           {},
}

func (r *etcdRun) handleChange(resp *etcd.Response, up chan<- config.TargetGroup) {
	_, isDelete := etcdDeleteActions[resp.Action]
	node := resp.Node
	if node.Dir {
		if isDelete {
			r.handleDeleteDir(node.Key, up)
		}
		return
	}

	dirKey := parentKey(node.Key)
	dir := r.dir(dirKey)

	if isDelete {
		delete(dir, node.Key)
	} else {
		dir[node.Key] = node.Value
	}

	up <- r.makeTargetGroup(dirKey)
}

func (r *etcdRun) makeTargetGroup(dirKey string) config.TargetGroup {
	var targets []model.LabelSet
	dir := r.dirs[dirKey]
	if dir != nil {
		// Eliminate duplicate values
		invDir := make(map[string]string)
		for key, val := range dir {
			invDir[val] = key
		}

		for val, key := range invDir {
			targets = append(targets, model.LabelSet{
				model.AddressLabel: model.LabelValue(val),
				etcdKeyLabel:       model.LabelValue(key),
			})
		}
	}

	return config.TargetGroup{
		Targets: targets,
		Labels: model.LabelSet{
			etcdDirectoryKeyLabel: model.LabelValue(dirKey),
		},
		Source: dirKey,
	}
}

func (r *etcdRun) handleDeleteDir(key string, up chan<- config.TargetGroup) {
	for k := range r.dirs {
		// Have we found the directory called key, or a subdir
		// of it?
		if k == key || (len(k) > len(key) && k[len(key)] == '/') {
			delete(r.dirs, k)
			up <- config.TargetGroup{Source: k}
		}
	}
}

func (r *etcdRun) doUpdatesTo(oldDirs etcdDirs, up chan<- config.TargetGroup) {
	for k, dir := range r.dirs {
		oldDir := oldDirs[k]
		if oldDir == nil || !etcdDirsEqual(dir, oldDir) {
			up <- r.makeTargetGroup(k)
		}
	}

	for k := range oldDirs {
		if r.dirs[k] == nil {
			up <- config.TargetGroup{Source: k}
		}
	}
}

func etcdDirsEqual(a, b map[string]string) bool {
	for k, av := range a {
		if bv, found := b[k]; !found || bv != av {
			return false
		}
	}

	for k := range b {
		if _, found := a[k]; !found {
			return false
		}
	}

	return true
}
