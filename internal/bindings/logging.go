package bindings

/*
#include <assert.h>
#include <stdlib.h>

#include <dqlite.h>

// Silence warnings.
extern int vasprintf(char **strp, const char *fmt, va_list ap);

// Go land callback for xLogf.
void dqliteLoggerLogfCb(uintptr_t handle, int level, char *msg);

// Implementation of xLogf.
static void dqliteLoggerLogf(void *ctx, int level, const char *format, ...) {
  uintptr_t handle;
  va_list args;
  char *msg;
  int err;

  assert(ctx != NULL);

  handle = (uintptr_t)ctx;

  va_start(args, format);
  err = vasprintf(&msg, format, args);
  va_end(args);
  if (err < 0) {
    // Ignore errors
    return;
  }

  dqliteLoggerLogfCb(handle, level, (char*)msg);

  free(msg);
}

// Constructor.
static dqlite_logger *dqliteLoggerAlloc(uintptr_t handle) {
  dqlite_logger *logger = malloc(sizeof *logger);

  if (logger == NULL) {
    return NULL;
  }

  logger->ctx = (void*)handle;
  logger->xLogf = dqliteLoggerLogf;

  return logger;
}
*/
import "C"
import (
	"github.com/CanonicalLtd/dqlite/internal/logging"
)

// Logging levels.
const (
	LogDebug = C.DQLITE_LOG_DEBUG
	LogInfo  = C.DQLITE_LOG_INFO
	LogWarn  = C.DQLITE_LOG_WARN
	LogError = C.DQLITE_LOG_ERROR
)

//export dqliteLoggerLogfCb
func dqliteLoggerLogfCb(handle C.uintptr_t, level C.int, msg *C.char) {
	f := loggerHandles[uintptr(handle)]

	message := C.GoString(msg)
	switch level {
	case LogDebug:
		f(logging.Debug, message)
	case LogInfo:
		f(logging.Info, message)
	case LogWarn:
		f(logging.Warn, message)
	case LogError:
		f(logging.Error, message)
	}
}

func newLogger(f logging.Func) *C.dqlite_logger {
	// Register the logger implementation and pass its handle to
	// dqliteLoggerInit.
	handle := loggerRegister(f)

	l := C.dqliteLoggerAlloc(C.uintptr_t(handle))
	if l == nil {
		panic("out of memory")
	}

	return l
}

// Map uintptr to logging.Func instances to avoid passing Go pointers to C.
//
// We do not protect this map with a lock since typically just one long-lived
// Logger instance should be registered (except for unit tests).
var loggerHandlesSerial uintptr = 100
var loggerHandles = map[uintptr]logging.Func{}

func loggerRegister(f logging.Func) uintptr {
	handle := loggerHandlesSerial

	loggerHandles[handle] = f
	loggerHandlesSerial++

	return handle
}

func loggerUnregister(handle uintptr) {
	delete(loggerHandles, handle)
}
