/*
 * aesdsocket - multithreaded socket server for AESD assignment 6.
 *
 * Listens on TCP port 9000. Each client connection is served by its own
 * thread: newline-terminated packets are appended to DATA_FILE and, after
 * each packet, the full file contents are sent back to the client.
 * A timer thread appends an RFC 2822 timestamp every 10 seconds.
 * SIGINT/SIGTERM shut down every thread, join them, and remove DATA_FILE.
 *
 * Usage: aesdsocket [-d]   (-d: run as a daemon after binding the port)
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#define PORT          "9000"
#define DATA_FILE     "/var/tmp/aesdsocketdata"
#define RECV_CHUNK    1024
#define SEND_CHUNK    1024
#define TIMESTAMP_SEC 10

static volatile sig_atomic_t exit_requested = 0;

/* Serializes every access to DATA_FILE (packet writes, read-back, timestamps). */
static pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Lets the timer thread sleep but wake immediately on shutdown. */
static pthread_mutex_t timer_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t timer_cond = PTHREAD_COND_INITIALIZER;
static bool timer_stop = false;

struct conn_thread {
    pthread_t thread;
    int client_fd;
    char client_ip[INET6_ADDRSTRLEN];
    volatile bool complete; /* set by the worker just before it returns */
    SLIST_ENTRY(conn_thread) entries;
};

SLIST_HEAD(thread_list, conn_thread);

static void signal_handler(int signo)
{
    (void)signo;
    exit_requested = 1;
}

/* Write all of buf to fd, retrying on partial writes. */
static int write_all(int fd, const char *buf, size_t len)
{
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

static int send_all(int fd, const char *buf, size_t len)
{
    while (len > 0) {
        ssize_t n = send(fd, buf, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

/*
 * Append one packet to DATA_FILE and send the whole file back to the client.
 * Both steps happen under file_mutex so the reply is consistent and packets
 * from different threads are never interleaved.
 */
static int handle_packet(int client_fd, const char *pkt, size_t len)
{
    int rc = 0;
    char buf[SEND_CHUNK];

    pthread_mutex_lock(&file_mutex);

    int fd = open(DATA_FILE, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0) {
        syslog(LOG_ERR, "open %s for append: %s", DATA_FILE, strerror(errno));
        rc = -1;
        goto out;
    }
    if (write_all(fd, pkt, len) < 0) {
        syslog(LOG_ERR, "write %s: %s", DATA_FILE, strerror(errno));
        rc = -1;
    }
    close(fd);
    if (rc < 0)
        goto out;

    fd = open(DATA_FILE, O_RDONLY);
    if (fd < 0) {
        syslog(LOG_ERR, "open %s for read: %s", DATA_FILE, strerror(errno));
        rc = -1;
        goto out;
    }
    ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        if (send_all(client_fd, buf, (size_t)n) < 0) {
            syslog(LOG_ERR, "send: %s", strerror(errno));
            rc = -1;
            break;
        }
    }
    if (n < 0) {
        syslog(LOG_ERR, "read %s: %s", DATA_FILE, strerror(errno));
        rc = -1;
    }
    close(fd);

out:
    pthread_mutex_unlock(&file_mutex);
    return rc;
}

/* Worker: receive packets until the client closes or an error occurs. */
static void *connection_thread(void *arg)
{
    struct conn_thread *ct = arg;
    char *pending = NULL; /* bytes received but not yet newline-terminated */
    size_t pending_len = 0;
    char chunk[RECV_CHUNK];

    syslog(LOG_INFO, "Accepted connection from %s", ct->client_ip);

    for (;;) {
        ssize_t n = recv(ct->client_fd, chunk, sizeof(chunk), 0);
        if (n == 0)
            break; /* client closed the connection */
        if (n < 0) {
            if (errno == EINTR)
                continue;
            syslog(LOG_ERR, "recv: %s", strerror(errno));
            break;
        }

        char *grown = realloc(pending, pending_len + (size_t)n);
        if (grown == NULL) {
            syslog(LOG_ERR, "realloc failed, dropping packet");
            free(pending);
            pending = NULL;
            pending_len = 0;
            continue;
        }
        pending = grown;
        memcpy(pending + pending_len, chunk, (size_t)n);
        pending_len += (size_t)n;

        /* Handle every complete packet in the buffer. */
        bool failed = false;
        char *nl;
        while ((nl = memchr(pending, '\n', pending_len)) != NULL) {
            size_t pkt_len = (size_t)(nl - pending) + 1;
            if (handle_packet(ct->client_fd, pending, pkt_len) < 0) {
                failed = true;
                break;
            }
            memmove(pending, pending + pkt_len, pending_len - pkt_len);
            pending_len -= pkt_len;
        }
        if (failed)
            break;
    }

    free(pending);
    syslog(LOG_INFO, "Closed connection from %s", ct->client_ip);
    ct->complete = true;
    return NULL;
}

/* Append "timestamp:<RFC 2822 time>\n" every TIMESTAMP_SEC until stopped. */
static void *timestamp_thread(void *arg)
{
    (void)arg;
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);

    pthread_mutex_lock(&timer_mutex);
    while (!timer_stop) {
        deadline.tv_sec += TIMESTAMP_SEC;
        int rc = 0;
        while (!timer_stop && rc != ETIMEDOUT)
            rc = pthread_cond_timedwait(&timer_cond, &timer_mutex, &deadline);
        if (timer_stop)
            break;

        char line[128];
        time_t now = time(NULL);
        struct tm tm_now;
        localtime_r(&now, &tm_now);
        size_t len = strftime(line, sizeof(line),
                              "timestamp:%a, %d %b %Y %H:%M:%S %z\n", &tm_now);
        if (len == 0)
            continue;

        pthread_mutex_lock(&file_mutex);
        int fd = open(DATA_FILE, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            if (write_all(fd, line, len) < 0)
                syslog(LOG_ERR, "timestamp write: %s", strerror(errno));
            close(fd);
        } else {
            syslog(LOG_ERR, "timestamp open: %s", strerror(errno));
        }
        pthread_mutex_unlock(&file_mutex);
    }
    pthread_mutex_unlock(&timer_mutex);
    return NULL;
}

/* Join and free every thread in the list that has finished. */
static void reap_completed(struct thread_list *head)
{
    struct conn_thread *ct = SLIST_FIRST(head);
    while (ct != NULL) {
        struct conn_thread *next = SLIST_NEXT(ct, entries);
        if (ct->complete) {
            pthread_join(ct->thread, NULL);
            close(ct->client_fd);
            SLIST_REMOVE(head, ct, conn_thread, entries);
            free(ct);
        }
        ct = next;
    }
}

static int open_listen_socket(void)
{
    struct addrinfo hints = {0};
    struct addrinfo *res = NULL;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    int rc = getaddrinfo(NULL, PORT, &hints, &res);
    if (rc != 0) {
        syslog(LOG_ERR, "getaddrinfo: %s", gai_strerror(rc));
        return -1;
    }

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        syslog(LOG_ERR, "socket: %s", strerror(errno));
        freeaddrinfo(res);
        return -1;
    }
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    if (bind(fd, res->ai_addr, res->ai_addrlen) < 0) {
        syslog(LOG_ERR, "bind: %s", strerror(errno));
        close(fd);
        freeaddrinfo(res);
        return -1;
    }
    freeaddrinfo(res);
    return fd;
}

static int daemonize(void)
{
    pid_t pid = fork();
    if (pid < 0) {
        syslog(LOG_ERR, "fork: %s", strerror(errno));
        return -1;
    }
    if (pid > 0)
        exit(EXIT_SUCCESS); /* parent: socket is bound, child carries on */

    if (setsid() < 0) {
        syslog(LOG_ERR, "setsid: %s", strerror(errno));
        return -1;
    }
    if (chdir("/") < 0) {
        syslog(LOG_ERR, "chdir: %s", strerror(errno));
        return -1;
    }
    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > STDERR_FILENO)
            close(devnull);
    }
    return 0;
}

int main(int argc, char *argv[])
{
    bool run_as_daemon = false;
    int opt;
    while ((opt = getopt(argc, argv, "d")) != -1) {
        if (opt == 'd') {
            run_as_daemon = true;
        } else {
            fprintf(stderr, "Usage: %s [-d]\n", argv[0]);
            return EXIT_FAILURE;
        }
    }

    openlog("aesdsocket", LOG_PID, LOG_USER);

    /* No SA_RESTART: accept() must return EINTR so main can shut down. */
    struct sigaction sa = {0};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0 || sigaction(SIGTERM, &sa, NULL) < 0) {
        syslog(LOG_ERR, "sigaction: %s", strerror(errno));
        closelog();
        return -1;
    }

    int listen_fd = open_listen_socket();
    if (listen_fd < 0) {
        closelog();
        return -1;
    }

    /* Fork before creating any thread: threads do not survive fork(). */
    if (run_as_daemon && daemonize() < 0) {
        close(listen_fd);
        closelog();
        return -1;
    }

    if (listen(listen_fd, 10) < 0) {
        syslog(LOG_ERR, "listen: %s", strerror(errno));
        close(listen_fd);
        closelog();
        return -1;
    }

    /*
     * Block SIGINT/SIGTERM while creating threads so they inherit the
     * blocked mask; only the main thread receives them, which interrupts
     * its blocking accept().
     */
    sigset_t block, orig;
    sigemptyset(&block);
    sigaddset(&block, SIGINT);
    sigaddset(&block, SIGTERM);

    /* Timer is started here, in the (possibly daemonized) child process. */
    pthread_t timer_tid;
    pthread_sigmask(SIG_BLOCK, &block, &orig);
    int rc = pthread_create(&timer_tid, NULL, timestamp_thread, NULL);
    pthread_sigmask(SIG_SETMASK, &orig, NULL);
    if (rc != 0) {
        syslog(LOG_ERR, "pthread_create timer: %s", strerror(rc));
        close(listen_fd);
        closelog();
        return -1;
    }

    struct thread_list threads;
    SLIST_INIT(&threads);

    while (!exit_requested) {
        struct sockaddr_storage client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue; /* loop re-checks exit_requested */
            syslog(LOG_ERR, "accept: %s", strerror(errno));
            continue;
        }

        struct conn_thread *ct = calloc(1, sizeof(*ct));
        if (ct == NULL) {
            syslog(LOG_ERR, "calloc failed, dropping connection");
            close(client_fd);
            continue;
        }
        ct->client_fd = client_fd;
        struct sockaddr_in *sin = (struct sockaddr_in *)&client_addr;
        inet_ntop(AF_INET, &sin->sin_addr, ct->client_ip, sizeof(ct->client_ip));

        pthread_sigmask(SIG_BLOCK, &block, &orig);
        rc = pthread_create(&ct->thread, NULL, connection_thread, ct);
        pthread_sigmask(SIG_SETMASK, &orig, NULL);
        if (rc != 0) {
            syslog(LOG_ERR, "pthread_create: %s", strerror(rc));
            close(client_fd);
            free(ct);
            continue;
        }
        SLIST_INSERT_HEAD(&threads, ct, entries);

        /* Free finished threads after starting the next one. */
        reap_completed(&threads);
    }

    syslog(LOG_INFO, "Caught signal, exiting");

    /* Ask each connection thread to exit: shutdown() unblocks its recv(). */
    struct conn_thread *ct;
    SLIST_FOREACH(ct, &threads, entries) {
        if (!ct->complete)
            shutdown(ct->client_fd, SHUT_RDWR);
    }
    while (!SLIST_EMPTY(&threads)) {
        ct = SLIST_FIRST(&threads);
        SLIST_REMOVE_HEAD(&threads, entries);
        pthread_join(ct->thread, NULL);
        close(ct->client_fd);
        free(ct);
    }

    pthread_mutex_lock(&timer_mutex);
    timer_stop = true;
    pthread_cond_signal(&timer_cond);
    pthread_mutex_unlock(&timer_mutex);
    pthread_join(timer_tid, NULL);

    close(listen_fd);
    remove(DATA_FILE);
    closelog();
    return 0;
}
