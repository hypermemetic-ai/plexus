// The combined WSS benchmark execs itself to create its fixture. Set its
// process affinity before Rust creates reactor and activation threads.
#define _GNU_SOURCE
#include <fcntl.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

__attribute__((constructor)) static void fixture_affinity(void) {
    char args[8192];
    int fd = open("/proc/self/cmdline", O_RDONLY);
    ssize_t n = fd < 0 ? -1 : read(fd, args, sizeof args);
    if (fd >= 0) close(fd);
    if (n <= 0) _exit(125);
    char *end = memchr(args, '\0', n);
    if (!end || end + 7 > args + n || memcmp(end + 1, "serve\0", 6)) return;
    char *cpus = getenv("WATCH_FIXTURE_CPUS");
    if (!cpus) _exit(125);
    cpu_set_t mask;
    CPU_ZERO(&mask);
    for (;;) {
        char *after;
        long cpu = strtol(cpus, &after, 10);
        if (after == cpus || cpu < 0 || cpu >= CPU_SETSIZE) _exit(125);
        CPU_SET(cpu, &mask);
        if (!*after) break;
        if (*after != ',') _exit(125);
        cpus = after + 1;
    }
    if (sched_setaffinity(0, sizeof mask, &mask)) _exit(125);
}
