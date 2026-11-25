// x86-64 Linux calibration for the reference server. Build with cc -O2 -pthread.
// Each selected CPU runs a fixed register-only workload. Compare its measured
// throughput with the instruction mix and CPU occupancy of real RPC tasks.
#define _GNU_SOURCE
#include <linux/perf_event.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

static pthread_barrier_t start;
struct sample {
    int cpu;
    double begin, end;
    uint64_t instructions, cycles;
};
static double seconds(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + 1e-9*t.tv_nsec;
}
static int counter(unsigned event, int leader) {
    struct perf_event_attr a = {.size=sizeof(a), .type=PERF_TYPE_HARDWARE,
        .config=event, .exclude_kernel=1, .exclude_hv=1,
        .read_format=PERF_FORMAT_TOTAL_TIME_ENABLED|PERF_FORMAT_TOTAL_TIME_RUNNING};
    int fd = syscall(SYS_perf_event_open, &a, 0, -1, leader, PERF_FLAG_FD_CLOEXEC);
    if (fd < 0) { perror("perf_event_open"); exit(1); }
    return fd;
}
static void sample(int fd, uint64_t value[3]) {
    if (read(fd, value, 3*sizeof(*value)) != 3*sizeof(*value)) { perror("PMU read"); exit(1); }
}
static uint64_t delta(uint64_t now[3], uint64_t prev[3]) {
    uint64_t elapsed=now[1]-prev[1], running=now[2]-prev[2];
    if (!running || !elapsed || running < elapsed*0.98) {
        fprintf(stderr,"insufficient PMU scheduling time\n"); exit(1);
    }
    return (uint64_t)((double)(now[0]-prev[0])*elapsed/running);
}
static void *work(void *ctx) {
    struct sample *s=ctx;
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(s->cpu,&set);
    if (sched_setaffinity(0,sizeof(set),&set)) { perror("affinity"); exit(1); }
    int inst=counter(PERF_COUNT_HW_INSTRUCTIONS,-1), cyc=counter(PERF_COUNT_HW_CPU_CYCLES,inst);
    uint64_t i0[3],i1[3],c0[3],c1[3];
    pthread_barrier_wait(&start);
    s->begin=seconds(); sample(inst,i0); sample(cyc,c0);
    unsigned long iterations=20000000, a=0,b=0,c=0,d=0,e=0,f=0,g=0,h=0;
    __asm__ volatile("1:\n .rept 8\n add $1,%1\n add $1,%2\n add $1,%3\n add $1,%4\n"
                     "add $1,%5\n add $1,%6\n add $1,%7\n add $1,%8\n .endr\n"
                     "sub $1,%0\n jnz 1b"
                     : "+r"(iterations),"+r"(a),"+r"(b),"+r"(c),"+r"(d),"+r"(e),"+r"(f),"+r"(g),"+r"(h) :: "cc");
    sample(cyc,c1); sample(inst,i1); s->end=seconds();
    if (a+b+c+d+e+f+g+h != 1280000000UL) abort();
    s->instructions=delta(i1,i0); s->cycles=delta(c1,c0);
    close(inst);close(cyc);
    return NULL;
}
int main(int argc,char **argv) {
    int n=argc-1;
    if (n<1 || n>CPU_SETSIZE) { fprintf(stderr,"usage: core-rate CPU [CPU ...]\n"); return 2; }
    struct sample *s=calloc(n,sizeof(*s)); pthread_t *ids=calloc(n,sizeof(*ids));
    if (!s || !ids || pthread_barrier_init(&start,NULL,n)) return 1;
    cpu_set_t seen; CPU_ZERO(&seen);
    for (int i=0;i<n;i++) {
        char *end; long id=strtol(argv[i+1],&end,10);
        if (*end || id<0 || id>=CPU_SETSIZE || CPU_ISSET(id,&seen)) return 2;
        s[i].cpu=id; CPU_SET(id,&seen);
        if (pthread_create(&ids[i],NULL,work,&s[i])) return 1;
    }
    double begin=1e30,end=0; uint64_t ins=0,cycles=0;
    for (int i=0;i<n;i++) {
        pthread_join(ids[i],NULL);
        if (s[i].begin<begin) begin=s[i].begin;
        if (s[i].end>end) end=s[i].end;
        ins+=s[i].instructions; cycles+=s[i].cycles;
    }
    printf("{\"cpus\":%d,\"instructions\":%llu,\"wall_s\":%.6f,\"instructions_s\":%.0f,\"ipc\":%.3f}\n",
           n,(unsigned long long)ins,end-begin,ins/(end-begin),(double)ins/cycles);
    free(s); free(ids);
}
