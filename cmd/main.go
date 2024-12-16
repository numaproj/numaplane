/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	clog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/isbservicerollout"
	"github.com/numaproj/numaplane/internal/controller/monovertexrollout"
	"github.com/numaproj/numaplane/internal/controller/numaflowcontroller"
	"github.com/numaproj/numaplane/internal/controller/numaflowcontrollerrollout"
	"github.com/numaproj/numaplane/internal/controller/pipelinerollout"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	// logger is the global logger for the controller-manager.
	numaLogger = logger.New().WithName("controller-manager")
	configPath = "/etc/numaplane" // Path in the volume mounted in the pod where yaml is present
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(apiv1.AddToScheme(scheme))

	utilruntime.Must(numaflowv1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", false,
		"If set the metrics endpoint is served securely")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	loadConfigs()

	ctx := logger.WithLogger(context.Background(), numaLogger)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOpts,
		},
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "numaplane-controller-lock",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// initialize the custom metrics with the global prometheus registry
	customMetrics := metrics.RegisterCustomMetrics()
	newRawConfig := metrics.AddMetricsTransportWrapper(customMetrics, mgr.GetConfig())

	if err := kubernetes.StartConfigMapWatcher(ctx, newRawConfig); err != nil {
		numaLogger.Fatal(err, "Failed to start configmap watcher")
	}

	if err := kubernetes.SetClientSets(newRawConfig); err != nil {
		numaLogger.Fatal(err, "Failed to set dynamic client")
	}

	//+kubebuilder:scaffold:builder

	pipelineRolloutReconciler := pipelinerollout.NewPipelineRolloutReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		customMetrics,
		mgr.GetEventRecorderFor(apiv1.RolloutPipelineName),
	)
	pipelinerollout.PipelineROReconciler = pipelineRolloutReconciler

	if err = pipelineRolloutReconciler.SetupWithManager(mgr); err != nil {
		numaLogger.Fatal(err, "Unable to set up PipelineRollout controller")
	}
	defer pipelineRolloutReconciler.Shutdown(ctx)

	kubectl := kubernetes.NewKubectl()
	numaflowControllerRolloutReconciler, err := numaflowcontrollerrollout.NewNumaflowControllerRolloutReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		newRawConfig,
		kubectl,
		customMetrics,
		mgr.GetEventRecorderFor(apiv1.RolloutNumaflowControllerName),
	)
	if err != nil {
		numaLogger.Fatal(err, "Unable to create NumaflowControllerRollout controller")
	}

	if err = numaflowControllerRolloutReconciler.SetupWithManager(mgr); err != nil {
		numaLogger.Fatal(err, "Unable to set up NumaflowControllerRollout controller")
	}

	isbServiceRolloutReconciler := isbservicerollout.NewISBServiceRolloutReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		customMetrics,
		mgr.GetEventRecorderFor(apiv1.RolloutISBSvcName),
	)

	if err = isbServiceRolloutReconciler.SetupWithManager(mgr); err != nil {
		numaLogger.Fatal(err, "Unable to set up ISBServiceRollout controller")
	}

	monoVertexRolloutReconciler := monovertexrollout.NewMonoVertexRolloutReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		customMetrics,
		mgr.GetEventRecorderFor(apiv1.RolloutMonoVertexName),
	)

	if err = monoVertexRolloutReconciler.SetupWithManager(mgr); err != nil {
		numaLogger.Fatal(err, "Unable to set up MonoVertexRollout controller")
	}

	numaflowControllerReconciler := numaflowcontroller.NewNumaflowControllerReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		customMetrics,
		mgr.GetEventRecorderFor(apiv1.NumaflowControllerName),
	)

	if err = numaflowControllerReconciler.SetupWithManager(mgr); err != nil {
		numaLogger.Fatal(err, "Unable to set up NumaflowController controller")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}

}

func loadConfigs() {

	configManager := config.GetConfigManagerInstance()
	err := configManager.LoadAllConfigs(func(err error) {
		numaLogger.Error(err, "Failed to reload global configuration file")
	},
		config.WithConfigsPath(configPath),
		config.WithConfigFileName("config"))
	if err != nil {
		numaLogger.Fatal(err, "Failed to load config file")
	}
	config, err := configManager.GetConfig()
	if err != nil {
		numaLogger.Fatal(err, "Failed to get config")
	}

	// anything we need to set based on our main Numaplane Config?
	numaLogger.SetLevel(config.LogLevel)
	logger.SetBaseLogger(numaLogger)
	clog.SetLogger(*numaLogger.LogrLogger)

}
