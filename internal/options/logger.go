package options

// Logger provides the logging capabilities for the module.
type Logger interface {
	Printf(format string, args ...any)
}
