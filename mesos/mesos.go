package mesos

import (
	"context"
	cTls "crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/job"
	"github.com/traefik/traefik/v3/pkg/observability/logs"
	"github.com/traefik/traefik/v3/pkg/provider"
	"github.com/traefik/traefik/v3/pkg/safe"

	// Register mesos zoo the detector

	_ "github.com/mesos/mesos-go/api/v0/detector/zoo"
)

// DefaultTemplateRule The default template for the default rule.
const DefaultTemplateRule = "Host(`{{ normalize .Name }}`)"

var (
	_ provider.Provider = (*Provider)(nil)
)

// Provider holds configuration of the provider.
type Provider struct {
	Endpoint              string        `Description:"Mesos server endpoint. You can also specify multiple endpoint for Mesos"`
	SSL                   bool          `Description:"Enable Endpoint SSL"`
	Principal             string        `Description:"Principal to authorize agains Mesos Manager"`
	Secret                string        `Description:"Secret authorize agains Mesos Manager"`
	PollInterval          time.Duration `Description:"Polling interval for endpoint." json:"pollInt"`
	PollTimeout           time.Duration `Description:"Polling timeout for endpoint." json:"pollTime"`
	DefaultRule           string        `Description:"Default rule." json:"defaultRule,omitempty" toml:"defaultRule,omitempty" yaml:"defaultRule,omitempty"`
	ForceUpdateInterval   time.Duration `Description:"Interval to force an update."`
	logger                zerolog.Logger
	mesosConfig           map[string]*MesosTasks
	defaultRuleTpl        *template.Template
	lastConfigurationHash uint64
	lastUpdate            time.Time
	agentEndpoints        map[string]mesosAgentEndpoint
	agentsLoaded          bool
	agentContainers       map[string]MesosAgentContainers
}

type mesosAgentEndpoint struct {
	hostname string
	port     int
}

// SetDefaults sets the default values.
func (p *Provider) SetDefaults() {
	p.Endpoint = "127.0.0.1:5050"
	p.SSL = false
	p.PollInterval = time.Duration(10 * time.Second)
	p.PollTimeout = time.Duration(10 * time.Second)
	p.DefaultRule = DefaultTemplateRule
	p.ForceUpdateInterval = time.Duration(10 * time.Minute)
	p.lastUpdate = time.Now()
}

// Init the provider.
func (p *Provider) Init() error {
	defaultRuleTpl, err := provider.MakeDefaultRuleTemplate(p.DefaultRule, nil)
	if err != nil {
		return fmt.Errorf("error while parsing default rule: %w", err)
	}

	p.defaultRuleTpl = defaultRuleTpl
	p.mesosConfig = make(map[string]*MesosTasks)
	return nil
}

// Provide allows the mesos provider to provide configurations to traefik
// using the given configuration channel.
func (p *Provider) Provide(configurationChan chan<- dynamic.Message, pool *safe.Pool) error {
	pool.GoCtx(func(routineCtx context.Context) {
		p.logger = log.Ctx(routineCtx).With().Str(logs.ProviderName, "mesos").Logger()
		ctxLog := p.logger.WithContext(routineCtx)

		// Add protocoll to the endpoint depends if SSL is enabled
		protocol := "http://" + p.Endpoint
		if p.SSL {
			protocol = "https://" + p.Endpoint
		}
		p.Endpoint = protocol

		p.logger.Info().Msgf("Connect Mesos Provider to: %s", p.Endpoint)

		operation := func() error {
			ctx, cancel := context.WithCancel(ctxLog)
			defer cancel()

			// load initial configuration
			if err := p.loadConfiguration(ctx, configurationChan); err != nil {
				return fmt.Errorf("failed to refresh mesos tasks: %w", err)
			}

			ticker := time.NewTicker(time.Duration(p.PollInterval))
			defer ticker.Stop()
			for {
				select {
				case <-routineCtx.Done():
					return nil
				case <-ticker.C:
				}
				if err := p.loadConfiguration(ctx, configurationChan); err != nil {
					return fmt.Errorf("failed to refresh mesos tasks: %w", err)
				}
			}
		}
		notify := func(err error, time time.Duration) {
			p.logger.Error().Msgf("Provider connection error %+v, retrying in %s", err, time)
		}

		err := backoff.RetryNotify(safe.OperationWithRecover(operation), job.NewBackOff(backoff.NewExponentialBackOff()), notify)
		if err != nil {
			p.logger.Error().Msgf("Cannot connect to Provider server: %+v", err)
		}
	})
	return nil
}

func (p *Provider) loadConfiguration(ctx context.Context, configurationChan chan<- dynamic.Message) error {
	tasks, err := p.getTasks(ctx)
	if err != nil {
		return fmt.Errorf("fetch mesos tasks: %w", err)
	}
	p.mesosConfig = make(map[string]*MesosTasks)
	p.agentEndpoints = make(map[string]mesosAgentEndpoint)
	p.agentContainers = make(map[string]MesosAgentContainers)
	p.agentsLoaded = false
	defer func() {
		p.mesosConfig = make(map[string]*MesosTasks)
	}()

	// collect all mesos tasks and combine the belong one.
	for _, task := range tasks.Tasks {
		if task.State == "TASK_RUNNING" {
			if task.Labels != nil {
				if p.checkTraefikLabels(task) {
					containerExists, err := p.checkContainer(task)
					if err != nil {
						return fmt.Errorf("check container for task %s: %w", task.ID, err)
					}
					if containerExists {
						containerName := task.ID
						if p.mesosConfig[containerName] == nil {
							p.mesosConfig[containerName] = &MesosTasks{}
						}
						p.mesosConfig[containerName].Tasks = append(p.mesosConfig[containerName].Tasks, task)
					}
				}
			}
		}
	}

	configuration := p.buildConfiguration(ctx)
	if configuration == nil {
		return fmt.Errorf("build traefik configuration")
	}

	configurationData, err := json.Marshal(configuration)
	if err != nil {
		return fmt.Errorf("marshal traefik configuration: %w", err)
	}

	fnvHasher := fnv.New64()
	if _, err = fnvHasher.Write(configurationData); err != nil {
		return fmt.Errorf("hash traefik configuration: %w", err)
	}

	timeNow := time.Now()
	timeDiff := timeNow.Sub(p.lastUpdate).Minutes()
	hash := fnvHasher.Sum64()
	if timeDiff < p.ForceUpdateInterval.Minutes() && hash == p.lastConfigurationHash {
		p.logger.Debug().Msg("nothing to update.")
		return nil
	}
	if timeDiff >= p.ForceUpdateInterval.Minutes() {
		p.logger.Info().Msgf("Force Update Traefik Config after %.1f minutes", timeDiff)
	}

	p.logger.Info().Msg("Update Traefik Config")
	configurationChan <- dynamic.Message{
		ProviderName:  "mesos",
		Configuration: configuration,
	}
	p.lastUpdate = timeNow
	p.lastConfigurationHash = hash

	return nil
}

func (p *Provider) checkTraefikLabels(task MesosTask) bool {
	for _, label := range task.Labels {
		if strings.Contains(label.Key, "traefik.") {
			return true
		}
	}
	return false
}

func (p *Provider) getTasks(ctx context.Context) (MesosTasks, error) {
	client := p.newHTTPClient()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.Endpoint+"/tasks?order=asc&limit=-1", nil)
	if err != nil {
		return MesosTasks{}, fmt.Errorf("create tasks request: %w", err)
	}
	req.Close = true
	req.SetBasicAuth(p.Principal, p.Secret)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if err != nil {
		return MesosTasks{}, fmt.Errorf("request tasks: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return MesosTasks{}, fmt.Errorf("received non-ok response code: %d", res.StatusCode)
	}

	p.logger.Debug().Msg("Get Data from Mesos")

	var tasks MesosTasks
	err = json.NewDecoder(res.Body).Decode(&tasks)
	if err != nil {
		return MesosTasks{}, fmt.Errorf("decode tasks response: %w", err)
	}
	return tasks, nil
}

func (p *Provider) checkContainer(task MesosTask) (bool, error) {
	agentHostname, agentPort, err := p.getAgent(task.SlaveID)

	if err != nil {
		return false, fmt.Errorf("get agent data: %w", err)
	}

	p.logger.Debug().Msg("CheckContainer: " + task.Name + " on agent (" + task.SlaveID + ")" + agentHostname + " with task.ID " + task.ID)

	if agentHostname != "" {
		containers, err := p.getContainersOfAgent(agentHostname, agentPort)
		if err != nil {
			return false, fmt.Errorf("get containers from agent %s: %w", agentHostname, err)
		}

		for _, a := range containers {
			p.logger.Debug().Msg(task.ID + " --CONTAINER--  " + a.ExecutorID)
			if a.ExecutorID == task.ID {
				return true, nil
			}
		}
	}

	return false, nil
}

func (p *Provider) getAgent(slaveID string) (string, int, error) {
	if p.agentsLoaded {
		endpoint, ok := p.agentEndpoints[slaveID]
		if !ok {
			return "", 0, nil
		}
		return endpoint.hostname, endpoint.port, nil
	}

	client := p.newHTTPClient()
	req, _ := http.NewRequest("GET", p.Endpoint+"/slaves/", nil)
	req.Close = true
	req.SetBasicAuth(p.Principal, p.Secret)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if err != nil {
		p.logger.Error().Msgf("Error during get agent: %s", err.Error())
		return "", 0, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("received non-ok response code: %d", res.StatusCode)
	}

	data, err := io.ReadAll(res.Body)
	var agents MesosAgent
	if err := json.Unmarshal(data, &agents); err != nil {
		p.logger.Error().Msg("getAgent: Error in AgentData from Mesos  " + p.Endpoint + " with error: " + err.Error())
		return "", 0, err
	}

	if p.agentEndpoints == nil {
		p.agentEndpoints = make(map[string]mesosAgentEndpoint)
	}
	for _, agent := range agents.Slaves {
		p.agentEndpoints[agent.ID] = mesosAgentEndpoint{hostname: agent.Hostname, port: agent.Port}
	}
	p.agentsLoaded = true

	endpoint, ok := p.agentEndpoints[slaveID]
	if !ok {
		return "", 0, nil
	}
	return endpoint.hostname, endpoint.port, nil
}

func (p *Provider) getContainersOfAgent(agentHostname string, agentPort int) (MesosAgentContainers, error) {
	cacheKey := net.JoinHostPort(agentHostname, strconv.Itoa(agentPort))
	if containers, ok := p.agentContainers[cacheKey]; ok {
		return containers, nil
	}

	// Add protocoll to the endpoint depends if SSL is enabled
	protocol := "http://"
	if p.SSL {
		protocol = "https://"
	}

	client := p.newHTTPClient()
	req, _ := http.NewRequest("GET", protocol+agentHostname+":"+strconv.Itoa(agentPort)+"/containers/", nil)
	req.Close = true
	req.SetBasicAuth(p.Principal, p.Secret)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if err != nil {
		p.logger.Error().Msgf("Error during get container: %s", err.Error())
		return MesosAgentContainers{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return MesosAgentContainers{}, fmt.Errorf("received non-ok response code: %d", res.StatusCode)
	}

	data, err := io.ReadAll(res.Body)
	var containers MesosAgentContainers
	if err := json.Unmarshal(data, &containers); err != nil {
		p.logger.Error().Msg("getContainersOfAgent: Error in ContainerAgentData from " + agentHostname + "  " + err.Error())
		return MesosAgentContainers{}, err
	}
	if p.agentContainers == nil {
		p.agentContainers = make(map[string]MesosAgentContainers)
	}
	p.agentContainers[cacheKey] = containers

	return containers, nil
}

func (p *Provider) newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: p.PollTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &cTls.Config{InsecureSkipVerify: true},
		},
	}
}
