/*
 * file: logger.c
 * auth: chenjianfei@daoke.me
 * date: Jun 30, 2014
 * desc: 
 */

#include "logger.h"

typedef struct log_context
{
    int32_t level;
    FILE* fp;
    pthread_mutex_t *mutex;
    char filename[PATH_MAX];
    uint64_t rotate_size;
    struct
    {
        uint64_t w_curr;
        uint64_t w_total;
    } stats;
} LogContext;

static LogContext g_log_context;

int32_t open_log(const char *filename, int32_t level, int8_t is_threadsafe, uint64_t rotate_size)
{
    if (strlen(filename) > PATH_MAX - 20) {
        fprintf(stderr, "log filename too long!");
        return -1;
    }
    strcpy(g_log_context.filename, filename);

    FILE *fp;
    if (strcmp(filename, "stdout") == 0) {
        fp = stdout;
    } else if (strcmp(filename, "stderr") == 0) {
        fp = stderr;
    } else {
        fp = fopen(filename, "a");
        if (fp == NULL) {
            return -1;
        }

        struct stat st;
        int ret = fstat(fileno(fp), &st);
        if (ret == -1) {
            fprintf(stderr, "fstat log file %s error!", filename);
            return -1;
        } else {
            g_log_context.rotate_size = rotate_size;
            g_log_context.stats.w_curr = st.st_size;
        }
    }

    g_log_context.fp = fp;
    g_log_context.level = level;

    if (is_threadsafe) {
        if (g_log_context.mutex) {
            pthread_mutex_destroy(g_log_context.mutex);
            free(g_log_context.mutex);
            g_log_context.mutex = NULL;
        }
        g_log_context.mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(g_log_context.mutex, NULL);
    }

    return 0;
}

void close_log()
{
    if (g_log_context.mutex) {
        pthread_mutex_destroy(g_log_context.mutex);
        free(g_log_context.mutex);
    }
    if (g_log_context.fp != stdin && g_log_context.fp != stdout) {
        fclose(g_log_context.fp);
    }
}

void set_log_level(int level)
{
    g_log_context.level = level;
}

static void rotate()
{
    fclose(g_log_context.fp);
    char newpath[PATH_MAX];
    time_t time;
    struct timeval tv;
    struct tm *tm;
    gettimeofday(&tv, NULL);
    time = tv.tv_sec;
    tm = localtime(&time);
    sprintf(newpath, "%s.%04d%02d%02d-%02d%02d%02d", g_log_context.filename,
            tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour,
            tm->tm_min, tm->tm_sec);

    int ret = rename(g_log_context.filename, newpath);
    if (ret == -1) {
        return;
    }
    g_log_context.fp = fopen(g_log_context.filename, "a");
    if (g_log_context.fp == NULL) {
        return;
    }
    g_log_context.stats.w_curr = 0;
}

inline static const char* level_name(int level)
{
    switch (level) {
        case LEVEL_FATAL:
            return "[FATAL] ";
        case LEVEL_ERROR:
            return "[ERROR] ";
        case LEVEL_WARN:
            return "[WARN ] ";
        case LEVEL_INFO:
            return "[INFO ] ";
        case LEVEL_DEBUG:
            return "[DEBUG] ";
        case LEVEL_TRACE:
            return "[TRACE] ";
    }
    return "";
}

#define LEVEL_NAME_LEN  8
#define LOG_BUF_LEN     4096

static int logv(int level, const char *fmt, va_list ap)
{
    if (g_log_context.level < level) {
        return 0;
    }

    char buf[LOG_BUF_LEN];
    int len;
    char *ptr = buf;

    time_t time;
    struct timeval tv;
    struct tm *tm;
    gettimeofday(&tv, NULL);
    time = tv.tv_sec;
    tm = localtime(&time);
    /* %3ld 在数值位数超过3位的时候不起作用, 所以这里转成int */
    len = sprintf(ptr, "%04d-%02d-%02d %02d:%02d:%02d.%03d ", tm->tm_year + 1900,
            tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec,
            (int) (tv.tv_usec / 1000));
    if (len < 0) {
        return -1;
    }
    ptr += len;

    memcpy(ptr, level_name(level), LEVEL_NAME_LEN);
    ptr += LEVEL_NAME_LEN;

    int space = sizeof(buf) - (ptr - buf) - 10;
    len = vsnprintf(ptr, space, fmt, ap);
    if (len < 0) {
        return -1;
    }
    ptr += len > space ? space : len;
    *ptr++ = '\n';
    *ptr = '\0';

    len = ptr - buf;
    // change to write(), without locking?
    if (g_log_context.mutex) {
        pthread_mutex_lock(g_log_context.mutex);
    }
    fwrite(buf, len, 1, g_log_context.fp);
    fflush(g_log_context.fp);

    g_log_context.stats.w_curr += len;
    g_log_context.stats.w_total += len;
    if (g_log_context.rotate_size > 0 && g_log_context.stats.w_curr > g_log_context.rotate_size) {
        rotate();
    }
    if (g_log_context.mutex) {
        pthread_mutex_unlock(g_log_context.mutex);
    }

    return len;
}

int log_write(int level, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int ret = logv(level, fmt, ap);
    va_end(ap);
    return ret;
}

