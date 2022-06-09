package registry_center

import (
	"testing"
	"time"
)

var req = &RequestRegister{
	Env:             "test",
	AppId:           "com.xx.testapp",
	Hostname:        "webapi",
	Addrs:           []string{"http://testapp.com"},
	Status:          1,
	Version:         "v1.0.0",
	LatestTimestamp: time.Now().Unix(),
	DirtyTimestamp:  0,
	Replication:     false,
}

func TestRegister(t *testing.T) {
	r := NewRegistry()
	instance := NewInstance(req)
	// test register
	app, _ := r.Register(instance, req.LatestTimestamp)
	t.Log(app)
	// test Fetch
	instances, _ := r.Fetch("test", "com.xx.testapp", 1, 0)
	t.Log(instances)
	// test cancle
	in, _ := r.Cancel("test", "com.xx.testapp", "webapi", time.Now().Unix())
	t.Log(in)
	// test fetch
	instances2, _ := r.Fetch("test", "com.xx.testapp", 1, 0)
	t.Log(instances2)
}
