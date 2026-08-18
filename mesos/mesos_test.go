package mesos

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

func TestLoadConfigurationRechecksContainerDiscovery(t *testing.T) {
	var containerAvailable atomic.Bool
	var containerRequests atomic.Int32

	agentServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/containers/", req.URL.Path)
		containerRequests.Add(1)
		rw.Header().Set("Content-Type", "application/json")
		if containerAvailable.Load() {
			_, err := rw.Write([]byte(`[{"executor_id":"synthetic-task-1"}]`))
			require.NoError(t, err)
			return
		}
		_, err := rw.Write([]byte(`[]`))
		require.NoError(t, err)
	}))
	t.Cleanup(agentServer.Close)

	agentURL, err := url.Parse(agentServer.URL)
	require.NoError(t, err)
	agentHost, agentPortText, err := net.SplitHostPort(agentURL.Host)
	require.NoError(t, err)
	agentPort, err := strconv.Atoi(agentPortText)
	require.NoError(t, err)

	const tasksJSON = `{"tasks":[{"id":"synthetic-task-1","name":"synthetic-service","slave_id":"synthetic-agent-1","state":"TASK_RUNNING","labels":[{"key":"traefik.http.routers.synthetic.rule","value":"Host(` + "`" + `synthetic.invalid` + "`" + `)"},{"key":"traefik.http.routers.synthetic.service","value":"synthetic-port"}],"statuses":[{"state":"TASK_STARTING","container_status":{"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"192.0.2.10"}]}]}}],"discovery":{"ports":{"ports":[{"number":8080,"name":"synthetic-port","protocol":"tcp"}]}}}]}`

	masterServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/tasks":
			_, err = rw.Write([]byte(tasksJSON))
			require.NoError(t, err)
		case "/slaves/":
			_, err = fmt.Fprintf(rw, `{"slaves":[{"id":"synthetic-agent-1","hostname":%q,"port":%d}]}`, agentHost, agentPort)
			require.NoError(t, err)
		default:
			http.NotFound(rw, req)
		}
	}))
	t.Cleanup(masterServer.Close)

	provider := &Provider{}
	provider.SetDefaults()
	provider.Endpoint = masterServer.URL
	provider.ForceUpdateInterval = time.Hour
	require.NoError(t, provider.Init())

	configurationChan := make(chan dynamic.Message, 2)
	require.NoError(t, provider.loadConfiguration(context.Background(), configurationChan))

	containerAvailable.Store(true)
	require.NoError(t, provider.loadConfiguration(context.Background(), configurationChan))

	assert.Equal(t, int32(2), containerRequests.Load())
	require.Len(t, configurationChan, 2)

	emptyMessage := <-configurationChan
	assert.Empty(t, emptyMessage.Configuration.HTTP.Routers)

	availableMessage := <-configurationChan
	require.Contains(t, availableMessage.Configuration.HTTP.Routers, "synthetic")
	require.Contains(t, availableMessage.Configuration.HTTP.Services, "synthetic-port")
	assert.Equal(t, "http://192.0.2.10:8080", availableMessage.Configuration.HTTP.Services["synthetic-port"].LoadBalancer.Servers[0].URL)
}

func TestLoadConfigurationDoesNotRemoveRoutesOnTaskFetchError(t *testing.T) {
	masterServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		http.Error(rw, "synthetic failure", http.StatusServiceUnavailable)
	}))
	t.Cleanup(masterServer.Close)

	provider := &Provider{}
	provider.SetDefaults()
	provider.Endpoint = masterServer.URL
	require.NoError(t, provider.Init())

	configurationChan := make(chan dynamic.Message, 1)
	err := provider.loadConfiguration(context.Background(), configurationChan)

	require.Error(t, err)
	assert.Empty(t, configurationChan)
}

func TestLoadConfigurationHonorsPollTimeout(t *testing.T) {
	masterServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, err := rw.Write([]byte(`{"tasks":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(masterServer.Close)

	provider := &Provider{}
	provider.SetDefaults()
	provider.Endpoint = masterServer.URL
	provider.PollTimeout = 20 * time.Millisecond
	require.NoError(t, provider.Init())

	started := time.Now()
	err := provider.loadConfiguration(context.Background(), make(chan dynamic.Message, 1))
	elapsed := time.Since(started)

	require.Error(t, err)
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestCheckContainerHonorsPollTimeoutWhileFetchingAgent(t *testing.T) {
	masterServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, err := rw.Write([]byte(`{"slaves":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(masterServer.Close)

	provider := &Provider{Endpoint: masterServer.URL, PollTimeout: 20 * time.Millisecond}
	started := time.Now()
	found, err := provider.checkContainer(MesosTask{SlaveID: "synthetic-agent-1"})
	elapsed := time.Since(started)

	require.Error(t, err)
	assert.False(t, found)
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestGetContainersOfAgentHonorsPollTimeout(t *testing.T) {
	agentServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, err := rw.Write([]byte(`[]`))
		require.NoError(t, err)
	}))
	t.Cleanup(agentServer.Close)

	agentURL, err := url.Parse(agentServer.URL)
	require.NoError(t, err)
	agentHost, agentPortText, err := net.SplitHostPort(agentURL.Host)
	require.NoError(t, err)
	agentPort, err := strconv.Atoi(agentPortText)
	require.NoError(t, err)

	provider := &Provider{PollTimeout: 20 * time.Millisecond}
	started := time.Now()
	_, err = provider.getContainersOfAgent(agentHost, agentPort)
	elapsed := time.Since(started)

	require.Error(t, err)
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestLoadConfigurationQueriesEachAgentOncePerPoll(t *testing.T) {
	var agentRequests atomic.Int32
	var containerRequests atomic.Int32

	agentServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		containerRequests.Add(1)
		_, err := rw.Write([]byte(`[{"executor_id":"synthetic-task-1"},{"executor_id":"synthetic-task-2"}]`))
		require.NoError(t, err)
	}))
	t.Cleanup(agentServer.Close)

	agentURL, err := url.Parse(agentServer.URL)
	require.NoError(t, err)
	agentHost, agentPortText, err := net.SplitHostPort(agentURL.Host)
	require.NoError(t, err)
	agentPort, err := strconv.Atoi(agentPortText)
	require.NoError(t, err)

	const tasksJSON = `{"tasks":[{"id":"synthetic-task-1","name":"synthetic-service-1","slave_id":"synthetic-agent-1","state":"TASK_RUNNING","labels":[{"key":"traefik.enable","value":"true"}]},{"id":"synthetic-task-2","name":"synthetic-service-2","slave_id":"synthetic-agent-1","state":"TASK_RUNNING","labels":[{"key":"traefik.enable","value":"true"}]}]}`
	masterServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/tasks":
			_, err = rw.Write([]byte(tasksJSON))
			require.NoError(t, err)
		case "/slaves/":
			agentRequests.Add(1)
			_, err = fmt.Fprintf(rw, `{"slaves":[{"id":"synthetic-agent-1","hostname":%q,"port":%d}]}`, agentHost, agentPort)
			require.NoError(t, err)
		default:
			http.NotFound(rw, req)
		}
	}))
	t.Cleanup(masterServer.Close)

	provider := &Provider{}
	provider.SetDefaults()
	provider.Endpoint = masterServer.URL
	require.NoError(t, provider.Init())

	require.NoError(t, provider.loadConfiguration(context.Background(), make(chan dynamic.Message, 1)))
	assert.Equal(t, int32(1), agentRequests.Load())
	assert.Equal(t, int32(1), containerRequests.Load())
}

func TestLoadConfigurationDoesNotRemoveRoutesOnAgentFetchError(t *testing.T) {
	const tasksJSON = `{"tasks":[{"id":"synthetic-task-1","name":"synthetic-service","slave_id":"synthetic-agent-1","state":"TASK_RUNNING","labels":[{"key":"traefik.enable","value":"true"}]}]}`
	masterServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/tasks":
			_, err := rw.Write([]byte(tasksJSON))
			require.NoError(t, err)
		case "/slaves/":
			http.Error(rw, "synthetic failure", http.StatusServiceUnavailable)
		default:
			http.NotFound(rw, req)
		}
	}))
	t.Cleanup(masterServer.Close)

	provider := &Provider{}
	provider.SetDefaults()
	provider.Endpoint = masterServer.URL
	require.NoError(t, provider.Init())

	configurationChan := make(chan dynamic.Message, 1)
	err := provider.loadConfiguration(context.Background(), configurationChan)

	require.Error(t, err)
	assert.Empty(t, configurationChan)
}
