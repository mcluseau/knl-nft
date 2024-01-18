package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"time"

	"github.com/cespare/xxhash"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var (
	appCtx, appCancel = context.WithCancel(context.Background())

	containerRuntimeEndpoint = envFlag(
		"runtime-endpoint", "Endpoint of CRI container runtime service",
		"CONTAINER_RUNTIME_ENDPOINT", "unix:///var/run/containerd/containerd.sock")
)

func envFlag(flagName, doc, envVar, defaultValue string) *string {
	value := os.Getenv(envVar)
	if value == "" {
		value = defaultValue
	}
	return flag.String(flagName, value, doc)
}

func main() {
	log.Logger = log.Output(zerolog.NewConsoleWriter())
	flag.Parse()

	conn, err := dial()
	if err != nil {
		log.Fatal().Err(err).Str("runtime-endpoint", *containerRuntimeEndpoint).Msg("failed to connect to CRI container runtime service")
	}

	runtimeService := cri.NewRuntimeServiceClient(conn)

	for range time.Tick(time.Second) {
		if conn == nil {
			conn, err = dial()
			if err != nil {
				log.Error().Err(err).Str("runtime-endpoint", *containerRuntimeEndpoint).Msg("failed to connect to CRI container runtime service")
				continue
			}
			runtimeService = cri.NewRuntimeServiceClient(conn)
		}

		if !run(runtimeService) {
			conn.Close()
			conn = nil
		}
	}
}

func dial() (conn *grpc.ClientConn, err error) {
	return grpc.DialContext(appCtx, *containerRuntimeEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}

var prevRulesHash uint64

func run(runtimeService cri.RuntimeServiceClient) (ok bool) {
	ctx, cancel := context.WithTimeout(appCtx, 5*time.Second)
	defer cancel()

	portMapTCP := new(bytes.Buffer)
	portMapUDP := new(bytes.Buffer)

	containersResp, err := runtimeService.ListContainers(ctx, &cri.ListContainersRequest{})
	if err != nil {
		log.Error().Err(err).Msg("failed to list containers")
		return
	}

	containers := containersResp.Containers
	sort.Slice(containers, func(i, j int) bool {
		ci, cj := containers[i], containers[j]
		if ci.CreatedAt != cj.CreatedAt {
			return ci.CreatedAt < cj.CreatedAt
		}
		return ci.Id < cj.Id
	})

	for _, ctr := range containers {
		portsStr := ctr.Annotations["io.kubernetes.container.ports"]
		if portsStr == "" {
			continue
		}

		log := log.With().Str("container-id", ctr.Id).Str("container-name", ctr.Metadata.Name).Logger()

		ports := make([]PortMapping, 0)
		if err := json.Unmarshal([]byte(portsStr), &ports); err != nil {
			log.Error().Err(err).Msg("invalid container ports")
			return
		}

		if len(ports) == 0 {
			continue
		}

		pod, err := runtimeService.PodSandboxStatus(ctx, &cri.PodSandboxStatusRequest{PodSandboxId: ctr.PodSandboxId})
		if err != nil {
			log.Error().Err(err).Str("pod-id", ctr.PodSandboxId).Msg("failed to get pod status")
			return
		}

		ip := pod.Status.Network.Ip
		if ip == "" {
			continue
		}

		for _, port := range ports {
			hostPort := port.HostPort
			if hostPort == 0 {
				hostPort = port.ContainerPort
			}

			mapping := "      " + strconv.Itoa(hostPort) + " : " + ip + " . " + strconv.Itoa(port.ContainerPort) + ",\n"
			switch port.Protocol {
			case "TCP":
				portMapTCP.WriteString(mapping)
			case "UDP":
				portMapUDP.WriteString(mapping)
			}
		}
	}

	buf := new(bytes.Buffer)
	buf.WriteString(`table container-hostports {}
delete table container-hostports;
table container-hostports {
  chain prerouting {
    type nat hook prerouting priority filter; policy accept;
`)

	if portMapTCP.Len() != 0 {
		buf.WriteString("    fib daddr type local dnat to tcp dport map @host-ports-tcp;\n")
	}
	if portMapUDP.Len() != 0 {
		buf.WriteString("    fib daddr type local dnat to udp dport map @host-ports-udp;\n")
	}
	buf.WriteString("  }\n")

	if portMapTCP.Len() != 0 {
		buf.WriteString("  map host-ports-tcp {\n    type inet_service : ipv4_addr . inet_service;\n    elements = {\n")
		portMapTCP.WriteTo(buf)
		buf.WriteString("    }\n  }\n")
	}
	if portMapUDP.Len() != 0 {
		buf.WriteString("  map host-ports-udp {\n    type inet_service : ipv4_addr . inet_service;\n    elements = {\n")
		portMapUDP.WriteTo(buf)
		buf.WriteString("    }\n  }\n")
	}

	buf.WriteString("}\n")

	hash := xxhash.Sum64(buf.Bytes())
	if hash == prevRulesHash {
		return true
	}

	// fmt.Println(buf)

	cmd := exec.Command("nft", "-f", "-")
	cmd.Stdin = buf
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatal().Err(err).Str("input", buf.String()).Msg("nft failed")
		return
	}

	log.Info().Msg("new nft rules applied")
	prevRulesHash = hash

	return true
}

type PortMapping struct {
	HostPort      int
	ContainerPort int
	Protocol      string
}
