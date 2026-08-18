package mesos

import (
	"context"
	"strings"

	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/label"
	"github.com/traefik/traefik/v3/pkg/provider"
)

func (p *Provider) buildConfiguration(ctx context.Context) *dynamic.Configuration {
	configurations := make(map[string]*dynamic.Configuration)
	labels := make(map[string]string)
	for _, tasks := range p.mesosConfig {
		var task MesosTask
		// search the running task
		for _, cTask := range tasks.Tasks {
			if cTask.State == "TASK_RUNNING" {
				task = cTask
			}
		}

		if task.Labels != nil {
			containerName := task.ID
			for _, label := range task.Labels {
				key := strings.ReplaceAll(label.Key, "__mesos_taskid__", strings.ReplaceAll(task.ID, ".", "_"))
				value := strings.ReplaceAll(label.Value, "__mesos_taskid__", strings.ReplaceAll(task.ID, ".", "_"))

				portName := p.getPortname(containerName)
				if portName != "" {
					key = strings.ReplaceAll(key, "__mesos_portname__", p.getPortname(containerName))
					value = strings.ReplaceAll(value, "__mesos_portname__", p.getPortname(containerName))
				}

				labels[key] = value
			}
			confFromLabel, err := label.DecodeConfiguration(labels)
			if err != nil {
				p.logger.Warn().Msgf("Ignore Error in DecodeConfiguration (%s): %s", task.Name, err.Error())
				continue
			}
			p.buildTCPServiceConfiguration(ctx, containerName, confFromLabel.TCP)
			provider.BuildTCPRouterConfiguration(ctx, confFromLabel.TCP)

			p.buildUDPServiceConfiguration(ctx, containerName, confFromLabel.UDP)
			provider.BuildUDPRouterConfiguration(ctx, confFromLabel.UDP)

			p.buildHTTPServiceConfiguration(ctx, containerName, confFromLabel.HTTP)

			model := struct {
				Name   string
				Labels map[string]string
			}{
				Name:   task.Name,
				Labels: labels,
			}
			provider.BuildRouterConfiguration(ctx, confFromLabel.HTTP, containerName, p.defaultRuleTpl, model)

			//res2B, _ = json.Marshal(confFromLabel)
			//fmt.Println(string(res2B))

			configurations[containerName] = confFromLabel
		}
	}

	return provider.Merge(ctx, provider.NameSortedConfigurations(configurations), provider.ResourceStrategyMerge)
}

// getPortname get the discovery portname
// the Mesos Task with the containerName.
func (p *Provider) getPortname(containerName string) string {
	for _, task := range p.mesosConfig[containerName].Tasks {
		for _, status := range task.Statuses {
			// the host ip is only visible during starting task. Have to find out why
			if status.State == "TASK_STARTING" {
				for _, network := range status.ContainerStatus.NetworkInfos {
					for _, ip := range network.IPAddresses {
						if ip.Protocol == "IPv4" && len(task.Discovery.Ports.Ports) > 0 {
							return task.Discovery.Ports.Ports[0].Name
						}
					}
				}
			}
		}
	}

	return ""
}
