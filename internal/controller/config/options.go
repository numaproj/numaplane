package config

type options struct {
	configsPath    string
	configFileName string
	fileType       string
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		fileType: "yaml",
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
