package registry_center

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Registry struct {
	apps map[string]*Application // key: (appId+env) 应用服务唯一标识
	lock sync.RWMutex
}

type Application struct {
	appId           string               // 应用服务唯一标识
	instances       map[string]*Instance // 记录服务实例instance信息，key为实例hostname（服务实例唯一标识）, value为实例结构类型
	latestTimestamp int64                // 记录更新时间
	lock            sync.RWMutex
}

type Instance struct {
	Env      string   `json:"env"`      // Env 服务环境标识，如 online、dev、test
	AppId    string   `json:"appId"`    // AppId 应用服务唯一标识
	Hostname string   `json:"hostname"` // 服务实例唯一标识
	Addrs    []string `json:"addrs"`    // 服务实例地址,可以是http或rpc地址
	Version  string   `json:"version"`  // 服务实例版本
	Status   uint32   `json:"status"`   // 服务实例状态

	RegTimestamp    int64 `json:"reg_timestamp"`    // 注册时间
	UpTimestamp     int64 `json:"up_timestamp"`     // 更新时间
	RenewTimestamp  int64 `json:"renew_timestamp"`  // 续约时间
	DirtyTimestamp  int64 `json:"dirty_timestamp"`  // 脏数据时间
	LatestTimestamp int64 `json:"latest_timestamp"` // 最后更新时间
}

func NewRegistry() *Registry {
	registry := &Registry{
		apps: make(map[string]*Application),
	}
	// 启动goroutine 检查并剔除没有续约的服务实例
	go registry.evictTask()
	return registry
}

func (r *Registry) evictTask() {
	tick := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-tick.C:
			r.evict()
		}
	}
}

// 遍历注册表的所有 apps，然后再遍历其中的 instances，如果当前时间减去实例上一次续约时间
// instance.RenewTimestamp 达到阈值（默认 90 秒），那么将其加入过期队列中。这里并没有直接将过期队列所有实例都取消，
// 考虑 GC 以及 本地时间漂移的因素，设定了一个剔除的上限 evictionLimit，随机剔除一些过期实例。
func (r *Registry) evict() {
	now := time.Now().UnixNano()
	var expiredInstances []*Instance
	apps := r.getAllApplications()
	// 注册表中所有实例的个数
	var registryLen int
	for _, app := range apps {
		allInstances := app.GetAllInstances()
		registryLen += len(allInstances)
		for _, instance := range allInstances {
			if now-instance.RenewTimestamp > int64(90*time.Second) {
				expiredInstances = append(expiredInstances, instance)
			}
		}
	}
	// 剔除上限数量
	evictionLimit := registryLen - int(float64(registryLen)*0.85)
	expiredLen := len(expiredInstances)
	if expiredLen > evictionLimit {
		expiredLen = evictionLimit
	}

	if expiredLen == 0 {
		return
	}
	for i := 0; i < expiredLen; i++ {
		j := i + rand.Intn(len(expiredInstances)-i)
		expiredInstances[i], expiredInstances[j] = expiredInstances[j], expiredInstances[i]
		expiredInstance := expiredInstances[i]
		r.Cancel(expiredInstance.Env, expiredInstance.AppId, expiredInstance.Hostname, now)
	}
}

func (r *Registry) getAllApplications() []*Application {
	r.lock.RLock()
	defer r.lock.RUnlock()
	apps := make([]*Application, 0, len(r.apps))
	for _, app := range r.apps {
		apps = append(apps, app)
	}
	return apps
}

// Register 服务注册
func (r *Registry) Register(instance *Instance, latestTimestamp int64) (*Application, error) {
	key := getKey(instance.AppId, instance.Env)
	r.lock.RLock()
	app, ok := r.apps[key]
	r.lock.RUnlock()
	if !ok { // new app
		app = NewApplication(instance.AppId)
	}
	// add instance
	_, isNew := app.AddInstance(instance, latestTimestamp)
	if isNew {
		// todo
	}
	// add into registry apps
	r.lock.Lock()
	r.apps[key] = app
	r.lock.Unlock()
	return app, nil
}

// Fetch 服务获取
func (r *Registry) Fetch(env, appid string, status uint32, latestTimestamp int64) ([]*Instance, error) {
	app, ok := r.getApplication(appid, env)
	if !ok {
		return nil, errors.New("app not found")
	}
	c, err := app.GetInstance(status, latestTimestamp)
	return c.Instances, err
}

// Cancel 服务下线
func (r *Registry) Cancel(env, appid, hostname string, latestTimestamp int64) (*Instance, error) {
	log.Println("action cancel...")
	// find app
	app, ok := r.getApplication(appid, env)
	if !ok {
		return nil, errors.New("app not found")
	}
	instance, ok, insLen := app.Cancel(hostname, latestTimestamp)
	if !ok {
		return nil, errors.New("instance not found")
	}
	// if instances is empty, delete app from apps
	if insLen == 0 {
		r.lock.Lock()
		delete(r.apps, getKey(appid, env))
		r.lock.Unlock()
	}
	return instance, nil
}

// Renew 服务续约
func (r *Registry) Renew(env, appid, hostname string) (*Instance, error) {
	app, ok := r.getApplication(appid, env)
	if !ok {
		return nil, errors.New("app not found")
	}
	in, ok := app.Renew(hostname)
	if !ok {
		return nil, errors.New("instance not found")
	}
	return in, nil
}

func (r *Registry) getApplication(appid, env string) (*Application, bool) {
	key := getKey(appid, env)
	r.lock.RLock()
	app, ok := r.apps[key]
	r.lock.RUnlock()
	return app, ok
}

func getKey(appid, env string) string {
	return fmt.Sprintf("%s-%s", appid, env)
}

func NewApplication(appid string) *Application {
	return &Application{
		appId:     appid,
		instances: make(map[string]*Instance),
	}
}
func (app *Application) AddInstance(in *Instance, latestTimestamp int64) (*Instance, bool) {
	app.lock.Lock()
	defer app.lock.Unlock()
	appIns, ok := app.instances[in.Hostname]
	if ok { // exist
		in.UpTimestamp = appIns.UpTimestamp
		// dirtytimestamp
		if in.DirtyTimestamp < appIns.DirtyTimestamp {
			log.Println("register exist dirty timestamp")
			in = appIns
		}
	}
	// add or update instances
	app.instances[in.Hostname] = in
	app.upLatestTimestamp(latestTimestamp)
	returnIns := new(Instance)
	*returnIns = *in
	return returnIns, !ok
}

// update app latest_timestamp
func (app *Application) upLatestTimestamp(latestTimestamp int64) {
	app.latestTimestamp = latestTimestamp
}

type FetchData struct {
	Instances       []*Instance `json:"instances"`
	LatestTimestamp int64       `json:"latest_timestamp"`
}

func (app *Application) GetInstance(status uint32, latestTime int64) (*FetchData, error) {
	app.lock.RLock()
	defer app.lock.RUnlock()
	if latestTime >= app.latestTimestamp {
		return nil, errors.New("latest timestamp is not latest")
	}
	fetchData := FetchData{
		Instances:       make([]*Instance, 0),
		LatestTimestamp: app.latestTimestamp,
	}
	var exists bool
	for _, instance := range app.instances {
		if status&instance.Status > 0 {
			exists = true
			newInstance := copyInstance(instance)
			fetchData.Instances = append(fetchData.Instances, newInstance)
		}
	}
	if !exists {
		return nil, errors.New("not exist condition instance")
	}
	return &fetchData, nil
}

func (app *Application) Cancel(hostname string, latestTimestamp int64) (*Instance, bool, int) {
	newInstance := new(Instance)
	app.lock.Lock()
	defer app.lock.Unlock()
	appIn, ok := app.instances[hostname]
	if !ok {
		return nil, ok, 0
	}
	// delete hostname
	delete(app.instances, hostname)
	appIn.LatestTimestamp = latestTimestamp
	app.upLatestTimestamp(latestTimestamp)
	*newInstance = *appIn
	return newInstance, true, len(app.instances)
}

func (app *Application) Renew(hostname string) (*Instance, bool) {
	app.lock.Lock()
	defer app.lock.Unlock()
	appIn, ok := app.instances[hostname]
	if !ok {
		return nil, ok
	}
	appIn.RenewTimestamp = time.Now().UnixNano()
	return copyInstance(appIn), true
}

// 获取所有*Instance
func (app *Application) GetAllInstances() []*Instance {
	app.lock.RLock()
	defer app.lock.RUnlock()
	rs := make([]*Instance, 0, len(app.instances))
	for _, instance := range app.instances {
		newInstance := new(Instance)
		*newInstance = *instance
		rs = append(rs, newInstance)
	}
	return rs
}

// deep copy
func copyInstance(src *Instance) *Instance {
	dst := new(Instance)
	*dst = *src
	// copy addrs
	dst.Addrs = make([]string, len(src.Addrs))
	for i, addr := range src.Addrs {
		dst.Addrs[i] = addr
	}
	return dst
}

type RequestRegister struct {
	Env             string   `form:"env"`
	AppId           string   `form:"appid"`
	Hostname        string   `form:"hostname"`
	Addrs           []string `form:"addrs[]"`
	Status          uint32   `form:"status"`
	Version         string   `form:"version"`
	LatestTimestamp int64    `form:"latest_timestamp"`
	DirtyTimestamp  int64    `form:"dirty_timestamp"` // other node send
	Replication     bool     `form:"replication"`     // other node send
}

func NewInstance(req *RequestRegister) *Instance {
	now := time.Now().UnixNano()
	instance := &Instance{
		Env:             req.Env,
		AppId:           req.AppId,
		Hostname:        req.Hostname,
		Addrs:           req.Addrs,
		Version:         req.Version,
		Status:          req.Status,
		RegTimestamp:    now,
		UpTimestamp:     now,
		RenewTimestamp:  now,
		DirtyTimestamp:  now,
		LatestTimestamp: now,
	}
	return instance
}
