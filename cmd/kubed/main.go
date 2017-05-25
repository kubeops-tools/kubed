package main

import (
	"github.com/appscode/go/flags"
	"github.com/appscode/go/hold"
	_ "github.com/appscode/k8s-addons/api/install"
	"github.com/appscode/kubed/pkg"
	"github.com/appscode/log"
	logs "github.com/appscode/log/golog"
	"github.com/spf13/pflag"
)

func main() {
	config := &pkg.Config{
		APITokenPath:          "/var/run/secrets/appscode/api-token",
		APIEndpoint:           "api.appscode.com:50077",
		InfluxSecretName:      "appscode-influx",
		InfluxSecretNamespace: "kube-system",
		EnablePromMonitoring:  false,
	}
	pflag.StringVar(&config.APITokenPath, "api-token", config.APITokenPath, "Endpoint of elasticsearch")
	pflag.StringVar(&config.Master, "master", config.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	pflag.StringVar(&config.KubeConfig, "kubeconfig", config.KubeConfig, "Path to kubeconfig file with authorization information (the master location is set by the master flag).")
	pflag.StringVar(&config.APIEndpoint, "api-endpoint", config.APIEndpoint, "appscode api server host:port")
	pflag.StringVar(&config.ClusterName, "cluster-name", config.ClusterName, "Name of Kubernetes cluster")
	pflag.StringVar(&config.ESEndpoint, "es-endpoint", config.ESEndpoint, "Endpoint of elasticsearch")
	pflag.StringVar(&config.InfluxSecretName, "influx-secret", config.InfluxSecretName, "Influxdb secret name")
	pflag.StringVar(&config.InfluxSecretNamespace, "influx-secret-namespace", config.InfluxSecretNamespace, "Influxdb secret namespace")
	pflag.BoolVar(&config.EnablePromMonitoring, "enable-prometheus-monitoring", config.EnablePromMonitoring, "Enable Prometheus monitoring")

	flags.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	if config.APIEndpoint == "" ||
		config.ClusterName == "" ||
		config.APITokenPath == "" {
		log.Fatalln("required flag not provided.")
	}

	log.Infoln("Starting Kubed Process...")
	go pkg.Run(config)

	hold.Hold()
}
