#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/personality.h>
#include <sys/ptrace.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>

static void fail(const char *operation) {
    perror(operation);
    exit(EXIT_FAILURE);
}

static int wait_child(pid_t child) {
    int status;
    while (waitpid(child, &status, 0) < 0) {
        if (errno != EINTR) fail("waitpid");
    }
    return status;
}

int main(void) {
    struct utsname host;
    if (uname(&host) < 0) fail("uname");
    printf("Hosted test container: %s %s %s\n", host.sysname, host.release, host.machine);
    fflush(stdout);

    int timezone = open("/etc/localtime", O_RDONLY);
    if (timezone < 0) fail("open /etc/localtime");
    close(timezone);

    int persona = personality(0xffffffffUL);
    if (persona < 0) fail("get personality");
    if (personality((unsigned long)persona | ADDR_NO_RANDOMIZE) < 0)
        fail("personality ADDR_NO_RANDOMIZE");
    if (personality((unsigned long)persona) < 0) fail("restore personality");

    struct io_uring_params params = {0};
    int ring = (int)syscall(__NR_io_uring_setup, 2, &params);
    if (ring < 0) fail("io_uring_setup");
    close(ring);

    pid_t child = fork();
    if (child < 0) fail("fork");
    if (child == 0) {
        if (ptrace(PTRACE_TRACEME, 0, NULL, NULL) < 0) {
            perror("ptrace PTRACE_TRACEME");
            _exit(EXIT_FAILURE);
        }
        raise(SIGSTOP);
        _exit(EXIT_SUCCESS);
    }
    int status = wait_child(child);
    if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGSTOP) {
        fprintf(stderr, "ptrace child did not stop as expected\n");
        return EXIT_FAILURE;
    }
    if (ptrace(PTRACE_CONT, child, NULL, NULL) < 0) fail("ptrace PTRACE_CONT");
    status = wait_child(child);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS) {
        fprintf(stderr, "ptrace child did not exit successfully\n");
        return EXIT_FAILURE;
    }
    puts("Preflight passed: localtime, personality, io_uring_setup, child ptrace");
    return EXIT_SUCCESS;
}
