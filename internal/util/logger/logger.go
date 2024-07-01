package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	"github.com/rs/zerolog"

	"github.com/numaproj/numaplane/internal/controller/config"
)

const (
	loggerFieldName      = "logger"
	loggerFieldSeparator = "."
	loggerDefaultName    = "numaplane"
	messageFieldName     = "msg"
	timestampFieldName   = "ts"
	defaultLevel         = InfoLevel
)

// infoLevelShift defines the value to shift by from the InfoLevel
const infoLevelShift = 3

/*
Log Level Mapping:
| NumaLogger semantic | logr verbosity  | zerolog        |
| ------------------- | --------------- | -------------- |
|    fatal            |    1            |    4           |
|    error            |    error (NA)   |    3           |
|    warn             |    2            |    2           |
|    info             |    3            |    1           |
|    debug            |    4            |    0           |
|    verbose          |    5            |    -2 (custom) |
*/
const (
	FatalLevel   = infoLevelShift - 2
	WarnLevel    = infoLevelShift - 1
	InfoLevel    = infoLevelShift
	DebugLevel   = infoLevelShift + 1
	VerboseLevel = infoLevelShift + 2
)

// The following map define the logr verbosity or NumaLogger semantic levels (constants above) mapping
// to the zerolog levels (https://github.com/rs/zerolog/blob/master/log.go#L129).
// This conversion/mapping is due to the following inverse relationship between zerolog levels and logr verbosity:
// - zerolog(4=Fatal) = "always want to see" => zerolog(-2=Verbose) = "only want to see rarely, ex: for debug purposes"
// - logrVerbosity(1) = "always want to see" => logrVerbosity(5) = "only want to see rarely, ex: for debug purposes"
var logrVerbosityToZerologLevelMap = map[int]zerolog.Level{
	FatalLevel:   4,
	WarnLevel:    2,
	InfoLevel:    1,
	DebugLevel:   0,
	VerboseLevel: -2,
}

type loggerKey struct{}

// NumaLogger is the struct containing a pointer to a logr.Logger instance.
type NumaLogger struct {
	LogrLogger *logr.Logger
	LogLevel   int
}

// LogSink implements logr.LogSink using zerolog as base logger.
type LogSink struct {
	l     *zerolog.Logger
	name  string
	depth int
}

func init() {
	// Global-level definitions for zerolog
	zerolog.MessageFieldName = messageFieldName
	zerolog.TimestampFieldName = timestampFieldName
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.CallerMarshalFunc = zerologCallerMarshalFunc
	zerolog.LevelFieldMarshalFunc = zerologLevelFieldMarshalFunc

	// Set zerolog global level to the most verbose level
	zerolog.SetGlobalLevel(getZerologLevelFromLogrVerbosity(VerboseLevel))
}

// New returns a new NumaLogger with a logr.Logger instance with a
// default setup for zerolog, os.Stdout writer, and info level.
func New() *NumaLogger {
	w := io.Writer(os.Stdout)

	// get the log level
	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		fmt.Println("error using global config to get log level: " + err.Error())
		return newNumaLogger(&w, nil)
	}
	lvl := globalConfig.LogLevel

	return newNumaLogger(&w, &lvl)
}

// newNumaLogger returns a new NumaLogger with a logr.Logger instance with a default setup for zerolog.
// The writer argument sets the output the logs will be written to. If it is nil, os.Stdout will be used.
// The level argument sets the log level value for this logger instance.
func newNumaLogger(writer *io.Writer, level *int) *NumaLogger {

	w := io.Writer(os.Stdout)
	if writer != nil {
		w = *writer
	}

	lvl := int(defaultLevel)
	if level != nil {
		lvl = *level
	}

	zl := zerolog.New(w)
	zl = setLoggerLevel(&zl, lvl).
		With().
		Caller().
		Timestamp().
		Logger()

	sink := &LogSink{l: &zl}
	ll := logr.New(sink).WithName(loggerDefaultName)

	return &NumaLogger{&ll, lvl}
}

// WithLogger returns a copy of parent context in which the
// value associated with logger key is the supplied logger.
func WithLogger(ctx context.Context, logger *NumaLogger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger.DeepCopy())
}

// FromContext returns the logger in the context.
// If there is no logger in context, a new one is created.
func FromContext(ctx context.Context) *NumaLogger {
	if logger, ok := ctx.Value(loggerKey{}).(*NumaLogger); ok {
		return logger.DeepCopy()
	}
	return New()
}

func (in *NumaLogger) DeepCopy() *NumaLogger {
	if in == nil {
		return nil
	}
	out := new(NumaLogger)
	out.LogLevel = in.LogLevel
	out.LogrLogger = new(logr.Logger)
	*(out.LogrLogger) = *(in.LogrLogger)
	return out
}

// WithName appends a given name to the logger.
func (nl *NumaLogger) WithName(name string) *NumaLogger {
	out := *nl
	ll := nl.LogrLogger.WithName(name)
	out.LogrLogger = &ll
	return &out
}

// WithValues appends additional key/value pairs to the logger.
func (nl *NumaLogger) WithValues(keysAndValues ...any) *NumaLogger {
	out := *nl
	ll := nl.LogrLogger.WithValues(keysAndValues)
	out.LogrLogger = &ll
	return &out
}

// WithCallDepth returns a Logger instance that offsets the call stack by the
// specified number of frames when logging call site information.
func (nl *NumaLogger) WithCallDepth(depth int) *NumaLogger {
	out := *nl
	ll := nl.LogrLogger.WithCallDepth(depth)
	out.LogrLogger = &ll
	return &out
}

// Error logs an error with a message and optional key/value pairs.
func (nl *NumaLogger) Error(err error, msg string, keysAndValues ...any) {
	nl.LogrLogger.WithCallDepth(1).Error(err, msg, keysAndValues...)
}

// Errorf logs an error with a formatted message with args.
func (nl *NumaLogger) Errorf(err error, msg string, args ...any) {
	nl.WithCallDepth(1).Error(err, fmt.Sprintf(msg, args...))
}

// Fatal logs an error with a message and optional key/value pairs. Then, exits with code 1.
func (nl *NumaLogger) Fatal(err error, msg string, keysAndValues ...any) {
	keysAndValues = append(keysAndValues, "error", err)
	// NOTE: -infoLevelShift is needed to offset the `level += infoLevelShift` in the LogSink Info implementation
	nl.LogrLogger.GetSink().Info(FatalLevel-infoLevelShift, msg, keysAndValues...)
	os.Exit(1)
}

// Fatalf logs an error with a formatted message with args. Then, exits with code 1.
func (nl *NumaLogger) Fatalf(err error, msg string, args ...any) {
	nl.WithCallDepth(1).Fatal(err, fmt.Sprintf(msg, args...))
}

// Warn logs a warning-level message with optional key/value pairs.
func (nl *NumaLogger) Warn(msg string, keysAndValues ...any) {
	// NOTE: -infoLevelShift is needed to offset the `level += infoLevelShift` in the LogSink Info implementation
	nl.LogrLogger.GetSink().Info(WarnLevel-infoLevelShift, msg, keysAndValues...)
}

// Warn logs a warning-level formatted message with args.
func (nl *NumaLogger) Warnf(msg string, args ...any) {
	nl.WithCallDepth(1).Warn(fmt.Sprintf(msg, args...))
}

// Info logs an info-level message with optional key/value pairs.
func (nl *NumaLogger) Info(msg string, keysAndValues ...any) {
	// NOTE: -infoLevelShift is needed to offset the `level += infoLevelShift` in the LogSink Info implementation
	nl.LogrLogger.GetSink().Info(InfoLevel-infoLevelShift, msg, keysAndValues...)
}

// Infof logs an info-level formatted message with args.
func (nl *NumaLogger) Infof(msg string, args ...any) {
	nl.WithCallDepth(1).Info(fmt.Sprintf(msg, args...))
}

// Debug logs a debug-level message with optional key/value pairs.
func (nl *NumaLogger) Debug(msg string, keysAndValues ...any) {
	// NOTE: -infoLevelShift is needed to offset the `level += infoLevelShift` in the LogSink Info implementation
	nl.LogrLogger.GetSink().Info(DebugLevel-infoLevelShift, msg, keysAndValues...)
}

// Debugf logs a debug-level formatted message with args.
func (nl *NumaLogger) Debugf(msg string, args ...any) {
	nl.WithCallDepth(1).Debug(fmt.Sprintf(msg, args...))
}

// Verbose logs a verbose-level message with optional key/value pairs.
func (nl *NumaLogger) Verbose(msg string, keysAndValues ...any) {
	// NOTE: -infoLevelShift is needed to offset the `level += infoLevelShift` in the LogSink Info implementation
	nl.LogrLogger.GetSink().Info(VerboseLevel-infoLevelShift, msg, keysAndValues...)
}

// Verbosef logs a verbose-level formatted message with args.
func (nl *NumaLogger) Verbosef(msg string, args ...any) {
	nl.WithCallDepth(1).Verbose(fmt.Sprintf(msg, args...))
}

// SetLevel sets/changes the log level.
func (nl *NumaLogger) SetLevel(level int) {
	nl.LogLevel = level
	sink := nl.LogrLogger.GetSink().(*LogSink)
	zl := setLoggerLevel(sink.l, level)
	sink.l = &zl
}

// Init receives optional information about the logr library
// and sets the call depth accordingly.
func (ls *LogSink) Init(ri logr.RuntimeInfo) {
	ls.depth = ri.CallDepth + 2
}

// Enabled tests whether this LogSink is enabled at the specified V-level and per-package.
func (ls *LogSink) Enabled(level int) bool {
	// TODO: this function should return true, not only based on level settings (global, log level, etc.),
	// but also based on caller package (per-module logging feature).

	// Needed for direct calls to logr.Logger.Enabled to make the starting level be InfoLevel (2)
	level += infoLevelShift

	zlLevel := getZerologLevelFromLogrVerbosity(level)
	return zlLevel >= ls.l.GetLevel() && zlLevel >= zerolog.GlobalLevel()
}

// Info logs a non-error message (msg) with the given key/value pairs as context and the specified level.
func (ls *LogSink) Info(level int, msg string, keysAndValues ...any) {
	// Needed for direct calls to logr.Logger.Info to make the starting level be InfoLevel (2)
	level += infoLevelShift

	zlEvent := ls.l.WithLevel(getZerologLevelFromLogrVerbosity(level))
	ls.log(zlEvent, msg, keysAndValues)
}

// Error logs an error, with the given message, and key/value pairs as context.
func (ls *LogSink) Error(err error, msg string, keysAndValues ...any) {
	zlEvent := ls.l.Err(err)
	ls.log(zlEvent, msg, keysAndValues)
}

// Reusable function to be used in LogSink Info and Error functions.
func (ls *LogSink) log(zlEvent *zerolog.Event, msg string, keysAndValues []any) {
	if zlEvent == nil {
		return
	}

	if ls.name != "" {
		zlEvent = zlEvent.Str(loggerFieldName, ls.name)
	}

	zlEvent.Fields(keysAndValues).
		CallerSkipFrame(ls.depth).
		Msg(msg)
}

// WithValues returns a new LogSink with additional key/value pairs.
func (ls LogSink) WithValues(keysAndValues ...any) logr.LogSink {
	// Not sure why variadic arg keysAndValues is an array of array instead of just an array
	idky := keysAndValues[0]

	zl := ls.l.With().Fields(idky).Logger()
	ls.l = &zl
	return &ls
}

// WithName returns a new LogSink with the specified name appended.
func (ls LogSink) WithName(name string) logr.LogSink {
	if ls.name != "" {
		ls.name += loggerFieldSeparator + name
	} else {
		ls.name = name
	}

	return &ls
}

// WithCallDepth returns a LogSink that will offset the call stack
// by the specified number of frames when logging call site information.
func (ls LogSink) WithCallDepth(depth int) logr.LogSink {
	ls.depth += depth
	return &ls
}

// getZerologLevelFromLogrVerbosity returns the zerolog level equivalent to the logr verbosity level.
func getZerologLevelFromLogrVerbosity(level int) zerolog.Level {
	return logrVerbosityToZerologLevelMap[level]
}

// setLoggerLevel sets the zerolog log level based on the given logr.Logger verbosity.
func setLoggerLevel(zl *zerolog.Logger, level int) zerolog.Logger {
	// If the level is less than FatalLevel, use the default level instead.
	// This could happen in case the level is coming from configmap but it was not set.
	if level < FatalLevel {
		level = defaultLevel
	}

	return zl.Level(getZerologLevelFromLogrVerbosity(level))
}

// zerologLevelFieldMarshalFunc adds a way to convert a custom zerolog.Level values to strings.
func zerologLevelFieldMarshalFunc(lvl zerolog.Level) string {
	switch lvl {
	case getZerologLevelFromLogrVerbosity(VerboseLevel):
		return "verbose"
	}
	return lvl.String()
}

// zerologCallerMarshalFunc customizes the caller by only extracting last 2 path levels.
func zerologCallerMarshalFunc(pc uintptr, file string, line int) string {
	dir := filepath.Dir(file)
	base := filepath.Base(dir)
	return fmt.Sprintf("%s/%s:%d", base, filepath.Base(file), line)
}
