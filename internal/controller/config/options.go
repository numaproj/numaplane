package config

type options struct {
	configsPath           string
	configFileName        string
	configFileType        string
	rolloutConfigFileName string
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		configFileType: "yaml",
	}
}

func WithConfigsPath(configsPath string) Option {
	return func(o *options) {
		o.configsPath = configsPath
	}
}

func WithConfigFileName(configFileName string) Option {
	return func(o *options) {
		o.configFileName = configFileName
	}
}

func WithConfigFileType(configFileType string) Option {
	return func(o *options) {
		o.configFileType = configFileType
	}
}

func WithRolloutConfigFileName(rolloutConfigFileName string) Option {
	return func(o *options) {
		o.rolloutConfigFileName = rolloutConfigFileName
	}
}
