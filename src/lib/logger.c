#include "logger.h"

void debugf(struct dqlite_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, DQLITE_LOG_DEBUG, format, args);
    va_end(args);
}

void infof(struct dqlite_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, DQLITE_LOG_INFO, format, args);
    va_end(args);
}

void warnf(struct dqlite_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, DQLITE_LOG_WARN, format, args);
    va_end(args);
}

void errorf(struct dqlite_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, DQLITE_LOG_ERROR, format, args);
    va_end(args);
}
