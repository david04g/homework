/*
 * httpserver.c
 *
 * A multi-threaded HTTP server for Assignment 4.
 *
 * Usage: ./httpserver [-t threads] <port>
 *
 * This version streams GET file contents from a temporary file
 * rather than buffering it all in memory.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include <ctype.h>
#include <signal.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>

// Include provided headers (assumed precompiled)
#include "iowrapper.h" // provides read_n_bytes, write_n_bytes, etc.
#include "listener_socket.h" // provides ls_new, ls_accept, ls_delete
#include "protocol.h" // For send_response and related definitions.
#include "rwlock.h" // provides rwlock_new, reader_lock, writer_lock, etc.
#include "queue.h" // provides queue_new, queue_push, queue_pop, queue_delete

/* --- Constants --- */
#define MAX_URI_LENGTH    1024
#define MAX_METHOD_LENGTH 16
#define MAX_REQUEST_SIZE  2048
#define BUFFER_SIZE       4096
#define FILE_BUFFER_SIZE  4096

/* --- Global Variables --- */
static queue_t *task_queue = NULL; // thread-safe queue for tasks
static Listener_Socket_t *global_socket = NULL; // listener socket
static pthread_mutex_t audit_log_mutex = PTHREAD_MUTEX_INITIALIZER; // for atomic audit log writes

/* --- File Lock Manager with Commit Ordering --- */
typedef struct file_lock_node {
    char *filepath;
    rwlock_t *lock;
    pthread_mutex_t commit_mutex;
    pthread_cond_t commit_cond;
    uint64_t next_commit; // next commit order to assign
    uint64_t current_commit; // next commit order to output
    struct file_lock_node *next;
} file_lock_node_t;

static file_lock_node_t *file_lock_list = NULL;
static pthread_mutex_t file_lock_manager_mutex = PTHREAD_MUTEX_INITIALIZER;

/* get_file_lock_node: returns (or creates) the commit node for a given filepath. */
static file_lock_node_t *get_file_lock_node(const char *filepath) {
    pthread_mutex_lock(&file_lock_manager_mutex);
    file_lock_node_t *node = file_lock_list;
    while (node) {
        if (strcmp(node->filepath, filepath) == 0) {
            pthread_mutex_unlock(&file_lock_manager_mutex);
            return node;
        }
        node = node->next;
    }
    file_lock_node_t *new_node = malloc(sizeof(file_lock_node_t));
    if (!new_node) {
        pthread_mutex_unlock(&file_lock_manager_mutex);
        return NULL;
    }
    new_node->filepath = strdup(filepath);
    new_node->lock = rwlock_new(WRITERS, 0);
    pthread_mutex_init(&new_node->commit_mutex, NULL);
    pthread_cond_init(&new_node->commit_cond, NULL);
    new_node->next_commit = 0;
    new_node->current_commit = 0;
    new_node->next = file_lock_list;
    file_lock_list = new_node;
    pthread_mutex_unlock(&file_lock_manager_mutex);
    return new_node;
}

/* get_file_lock: convenience function to return just the rwlock for a file */
static rwlock_t *get_file_lock(const char *filepath) {
    file_lock_node_t *node = get_file_lock_node(filepath);
    return node ? node->lock : NULL;
}

/* --- Helper: valid_header_key --- */
static bool valid_header_key(const char *key) {
    if (!key || !isalpha((unsigned char) key[0]))
        return false;
    for (int i = 0; key[i] != '\0'; i++) {
        char c = key[i];
        if (!isalnum((unsigned char) c) && c != '-' && c != '.')
            return false;
    }
    return true;
}

/* --- Helper: parse_http_request --- */
static int parse_http_request(
    const char *header, char *method_out, char *uri_out, int *content_len) {
    *content_len = -1;
    if (strstr(header, "\r\n\r\n") == NULL)
        return 400;

    char *req_copy = strdup(header);
    if (!req_copy)
        return 500;
    char *saveptr;
    char *line = strtok_r(req_copy, "\r\n", &saveptr);
    if (!line) {
        free(req_copy);
        return 400;
    }

    char req_method[MAX_METHOD_LENGTH] = { 0 }, req_uri[MAX_URI_LENGTH] = { 0 },
         req_version[16] = { 0 };
    if (sscanf(line, "%15s %1023s %15s", req_method, req_uri, req_version) != 3) {
        free(req_copy);
        return 400;
    }
    // Check method is uppercase letters only
    for (int i = 0; req_method[i]; i++) {
        if (req_method[i] < 'A' || req_method[i] > 'Z') {
            free(req_copy);
            return 400;
        }
    }
    if (strcmp(req_method, "GET") != 0 && strcmp(req_method, "PUT") != 0) {
        free(req_copy);
        return 501;
    }
    if (strncmp(req_version, "HTTP/", 5) != 0) {
        free(req_copy);
        return 400;
    }
    int major, minor;
    if (sscanf(req_version + 5, "%d.%d", &major, &minor) != 2) {
        free(req_copy);
        return 400;
    }
    if (!(major == 1 && minor == 1)) {
        free(req_copy);
        return (major == 1 && minor >= 0 && minor <= 9) ? 505 : 400;
    }

    strncpy(method_out, req_method, MAX_METHOD_LENGTH - 1);
    strncpy(uri_out, req_uri, MAX_URI_LENGTH - 1);

    int has_content_length = 0;
    while ((line = strtok_r(NULL, "\r\n", &saveptr)) != NULL) {
        if (line[0] == '\0')
            break;
        char *colon = strchr(line, ':');
        if (!colon) {
            free(req_copy);
            return 400;
        }
        *colon = '\0';
        char *key = line;
        char *value = colon + 1;
        while (*key && isspace((unsigned char) *key))
            key++;
        while (*value && isspace((unsigned char) *value))
            value++;
        if (!valid_header_key(key)) {
            free(req_copy);
            return 400;
        }
        if (strcasecmp(key, "Content-Length") == 0) {
            int cl = atoi(value);
            if (cl < 0) {
                free(req_copy);
                return 400;
            }
            *content_len = cl;
            has_content_length = 1;
        }
    }
    if (strcmp(req_method, "PUT") == 0 && !has_content_length) {
        free(req_copy);
        return 400;
    }
    free(req_copy);
    return 0;
}

/* --- Task Structure --- */
typedef struct task {
    int client_socket;
    uint64_t order; // not used in global ordering but reserved for future
} task_t;

/* --- Response Info Structure --- */
typedef struct response_info {
    int status_code;
    char method[MAX_METHOD_LENGTH];
    char uri[MAX_URI_LENGTH];
    char request_id[64];

    // For simpler code, we still hold the response headers in memory:
    char *response_buffer;
    size_t response_length;

    // If we need to stream a file after commit ordering, store info here:
    char *temp_filename; // The temp spool file name (for GET)
    off_t file_size; // Size of the original file
    bool need_file_stream; // If true, we should stream after sending headers

    int client_socket;
    uint64_t commit_order;
    struct file_lock_node *commit_node;
} response_info_t;

/* --- Utility: write_all --- */
static ssize_t write_all(int fd, const char *buf, size_t count) {
    ssize_t total_written = 0;
    while (total_written < (ssize_t) count) {
        ssize_t written = write(fd, buf + total_written, count - total_written);
        if (written <= 0)
            return -1;
        total_written += written;
    }
    return total_written;
}

/*
 * spool_file_to_temp:
 *   Reads `src_path` while holding the read lock, writes it to a temporary file in chunks,
 *   and returns the temp filename plus total size read.
 *   Returns 0 on success, or a negative code on error.
 */
static int spool_file_to_temp(const char *src_path, char **temp_path_out, off_t *size_out) {
    *temp_path_out = NULL;
    *size_out = 0;

    // Open the source file
    int sfd = open(src_path, O_RDONLY);
    if (sfd < 0) {
        return -1;
    }

    // Create a temporary file name
    // (In a real system, you might want mkstemp, but here's a simpler approach.)
    char tmp_name[] = "/tmp/httpserver_temp_XXXXXX";
    int tfd = mkstemp(tmp_name);
    if (tfd < 0) {
        close(sfd);
        return -1;
    }

    // Copy in chunks
    char buffer[FILE_BUFFER_SIZE];
    off_t total_copied = 0;
    ssize_t bytes_read;
    while ((bytes_read = read(sfd, buffer, sizeof(buffer))) > 0) {
        ssize_t wr = write_all(tfd, buffer, bytes_read);
        if (wr < 0) {
            close(sfd);
            close(tfd);
            unlink(tmp_name);
            return -1;
        }
        total_copied += bytes_read;
    }
    close(sfd);
    close(tfd);

    // Assign outputs
    *temp_path_out = strdup(tmp_name);
    *size_out = total_copied;

    // We’ll re-open and read this file in the worker after commit ordering
    return 0;
}

/* --- process_connection_ordered --- */
static response_info_t process_connection_ordered(int client_socket) {
    response_info_t resp;
    memset(&resp, 0, sizeof(resp));
    resp.client_socket = client_socket;

    char request_buf[MAX_REQUEST_SIZE];
    ssize_t total_read = 0;

    // Read the request headers (and possibly part of the body)
    while (total_read < MAX_REQUEST_SIZE - 1) {
        ssize_t bytes
            = read(client_socket, request_buf + total_read, MAX_REQUEST_SIZE - total_read - 1);
        if (bytes < 0) {
            // Prepare a 400 response
            resp.status_code = 400;
            strcpy(resp.method, "UNKNOWN");
            strcpy(resp.uri, "/");
            strcpy(resp.request_id, "0");
            const char *msg = "Bad Request\n";
            char header[256];
            size_t cl = strlen(msg);
            snprintf(header, sizeof(header),
                "HTTP/1.1 400 Bad Request\r\nContent-Length: %zu\r\n\r\n", cl);
            size_t hl = strlen(header);
            resp.response_length = hl + cl;
            resp.response_buffer = malloc(resp.response_length);
            memcpy(resp.response_buffer, header, hl);
            memcpy(resp.response_buffer + hl, msg, cl);
            return resp;
        }
        if (bytes == 0)
            break;
        total_read += bytes;
        request_buf[total_read] = '\0';
        // Look for header terminator
        if (strstr(request_buf, "\r\n\r\n") != NULL)
            break;
    }

    if (strstr(request_buf, "\r\n\r\n") == NULL) {
        // Did not find full headers
        resp.status_code = 400;
        strcpy(resp.method, "UNKNOWN");
        strcpy(resp.uri, "/");
        strcpy(resp.request_id, "0");
        const char *msg = "Bad Request\n";
        char header[256];
        size_t cl = strlen(msg);
        snprintf(
            header, sizeof(header), "HTTP/1.1 400 Bad Request\r\nContent-Length: %zu\r\n\r\n", cl);
        size_t hl = strlen(header);
        resp.response_length = hl + cl;
        resp.response_buffer = malloc(resp.response_length);
        memcpy(resp.response_buffer, header, hl);
        memcpy(resp.response_buffer + hl, msg, cl);
        return resp;
    }

    char *header_end = strstr(request_buf, "\r\n\r\n");
    size_t header_length = header_end - request_buf + 4;
    if (header_length >= sizeof(request_buf))
        header_length = sizeof(request_buf) - 1;

    char header_buf[MAX_REQUEST_SIZE];
    memcpy(header_buf, request_buf, header_length);
    header_buf[header_length] = '\0';

    char method[MAX_METHOD_LENGTH] = { 0 }, uri[MAX_URI_LENGTH] = { 0 };
    int content_length_val = 0;
    int parse_result = parse_http_request(header_buf, method, uri, &content_length_val);
    if (parse_result != 0) {
        // Send error response
        resp.status_code = parse_result;
        strncpy(resp.method, method, MAX_METHOD_LENGTH - 1);
        strncpy(resp.uri, uri, MAX_URI_LENGTH - 1);
        strcpy(resp.request_id, "0");
        const char *msg = (parse_result == 400)   ? "Bad Request\n"
                          : (parse_result == 501) ? "Not Implemented\n"
                          : (parse_result == 505) ? "Version Not Supported\n"
                                                  : "Error\n";
        char header_out[256];
        size_t ml = strlen(msg);
        snprintf(header_out, sizeof(header_out), "HTTP/1.1 %d %s\r\nContent-Length: %zu\r\n\r\n",
            parse_result,
            (parse_result == 400)   ? "Bad Request"
            : (parse_result == 501) ? "Not Implemented"
            : (parse_result == 505) ? "Version Not Supported"
                                    : "Error",
            ml);
        size_t hl = strlen(header_out);
        resp.response_length = hl + ml;
        resp.response_buffer = malloc(resp.response_length);
        memcpy(resp.response_buffer, header_out, hl);
        memcpy(resp.response_buffer + hl, msg, ml);
        return resp;
    }

    // Grab Request-Id if present
    char request_id[64] = "0";
    {
        char *p = strcasestr(header_buf, "Request-Id:");
        if (p) {
            p += strlen("Request-Id:");
            while (*p && isspace((unsigned char) *p))
                p++;
            int i = 0;
            while (*p && *p != '\r' && *p != '\n' && i < 63) {
                request_id[i++] = *p;
                p++;
            }
            request_id[i] = '\0';
        }
    }
    strncpy(resp.method, method, MAX_METHOD_LENGTH - 1);
    strncpy(resp.uri, uri, MAX_URI_LENGTH - 1);
    strncpy(resp.request_id, request_id, 63);

    // Figure out how many bytes of body we already read
    size_t initial_body_size = total_read - header_length;
    char *initial_body = request_buf + header_length;

    char file_path[128] = { 0 };
    if (strcmp(method, "GET") == 0 || strcmp(method, "PUT") == 0) {
        snprintf(file_path, sizeof(file_path), ".%s", uri);
    }

    if (strcmp(method, "GET") == 0) {
        rwlock_t *flock = get_file_lock(file_path);
        if (flock)
            reader_lock(flock);

        struct stat path_stat;
        if (stat(file_path, &path_stat) != 0) {
            if (flock)
                reader_unlock(flock);
            // 404
            resp.status_code = 404;
            const char *msg = "Not Found\n";
            char header_out[256];
            size_t ml = strlen(msg);
            snprintf(header_out, sizeof(header_out),
                "HTTP/1.1 404 Not Found\r\nContent-Length: %zu\r\n\r\n", ml);
            size_t hl = strlen(header_out);
            resp.response_length = hl + ml;
            resp.response_buffer = malloc(resp.response_length);
            memcpy(resp.response_buffer, header_out, hl);
            memcpy(resp.response_buffer + hl, msg, ml);
            return resp;
        }

        // Check that it's a regular file, readable, etc.
        if (S_ISDIR(path_stat.st_mode) || access(file_path, R_OK) != 0) {
            if (flock)
                reader_unlock(flock);
            // 403
            resp.status_code = 403;
            const char *msg = "Forbidden\n";
            char header_out[256];
            size_t ml = strlen(msg);
            snprintf(header_out, sizeof(header_out),
                "HTTP/1.1 403 Forbidden\r\nContent-Length: %zu\r\n\r\n", ml);
            size_t hl = strlen(header_out);
            resp.response_length = hl + ml;
            resp.response_buffer = malloc(resp.response_length);
            memcpy(resp.response_buffer, header_out, hl);
            memcpy(resp.response_buffer + hl, msg, ml);
            return resp;
        }

        // Spool the file contents into a temporary file (to avoid huge memory usage):
        if (flock) {
            // spool file while we have the read lock
            if (spool_file_to_temp(file_path, &resp.temp_filename, &resp.file_size) < 0) {
                reader_unlock(flock);
                // 500
                resp.status_code = 500;
                const char *msg = "Internal Server Error\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            }

            reader_unlock(flock);
        }

        // Build the response headers; do NOT include the file data
        char header_out[256];
        snprintf(header_out, sizeof(header_out), "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n",
            (long) resp.file_size);
        size_t hl = strlen(header_out);
        resp.response_buffer = malloc(hl);
        memcpy(resp.response_buffer, header_out, hl);
        resp.response_length = hl;

        resp.status_code = 200;
        resp.need_file_stream = true; // We'll stream after commit wait
    } else if (strcmp(method, "PUT") == 0) {
        if (content_length_val < 0) {
            // 400
            resp.status_code = 400;
            const char *msg = "Bad Request\n";
            char header_out[256];
            size_t ml = strlen(msg);
            snprintf(header_out, sizeof(header_out),
                "HTTP/1.1 400 Bad Request\r\nContent-Length: %zu\r\n\r\n", ml);
            size_t hl = strlen(header_out);
            resp.response_length = hl + ml;
            resp.response_buffer = malloc(resp.response_length);
            memcpy(resp.response_buffer, header_out, hl);
            memcpy(resp.response_buffer + hl, msg, ml);
            return resp;
        }

        rwlock_t *flock = get_file_lock(file_path);
        if (flock)
            writer_lock(flock);

        struct stat path_stat;
        bool file_existed = false;
        if (stat(file_path, &path_stat) == 0 && S_ISREG(path_stat.st_mode)) {
            file_existed = true;
        }
        if (stat(file_path, &path_stat) == 0 && S_ISDIR(path_stat.st_mode)) {
            if (flock)
                writer_unlock(flock);
            // 403
            resp.status_code = 403;
            const char *msg = "Forbidden\n";
            char header_out[256];
            size_t ml = strlen(msg);
            snprintf(header_out, sizeof(header_out),
                "HTTP/1.1 403 Forbidden\r\nContent-Length: %zu\r\n\r\n", ml);
            size_t hl = strlen(header_out);
            resp.response_length = hl + ml;
            resp.response_buffer = malloc(resp.response_length);
            memcpy(resp.response_buffer, header_out, hl);
            memcpy(resp.response_buffer + hl, msg, ml);
            return resp;
        }

        int fd = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            if (flock)
                writer_unlock(flock);
            if (errno == EACCES) {
                resp.status_code = 403;
                const char *msg = "Forbidden\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 403 Forbidden\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            } else if (errno == ENOENT) {
                resp.status_code = 404;
                const char *msg = "Not Found\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 404 Not Found\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            } else {
                resp.status_code = 500;
                const char *msg = "Internal Server Error\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            }
        }

        // Write any initial body we already read
        ssize_t total_written = 0;
        if (initial_body_size > 0) {
            ssize_t written = write_all(fd, initial_body, initial_body_size);
            if (written != (ssize_t) initial_body_size) {
                close(fd);
                if (flock)
                    writer_unlock(flock);
                resp.status_code = 500;
                const char *msg = "Internal Server Error\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            }
            total_written += initial_body_size;
        }

        // Read rest of body from socket
        char buffer[BUFFER_SIZE];
        while ((size_t) total_written < (size_t) content_length_val) {
            size_t to_read = ((size_t) content_length_val - (size_t) total_written);
            if (to_read > BUFFER_SIZE) {
                to_read = BUFFER_SIZE;
            }
            ssize_t bytes_read = read(client_socket, buffer, to_read);
            if (bytes_read <= 0) {
                close(fd);
                if (flock)
                    writer_unlock(flock);
                resp.status_code = 500;
                const char *msg = "Internal Server Error\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            }
            ssize_t bytes_written = write_all(fd, buffer, bytes_read);
            if (bytes_written != bytes_read) {
                close(fd);
                if (flock)
                    writer_unlock(flock);
                resp.status_code = 500;
                const char *msg = "Internal Server Error\n";
                char header_out[256];
                size_t ml = strlen(msg);
                snprintf(header_out, sizeof(header_out),
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: %zu\r\n\r\n", ml);
                size_t hl = strlen(header_out);
                resp.response_length = hl + ml;
                resp.response_buffer = malloc(resp.response_length);
                memcpy(resp.response_buffer, header_out, hl);
                memcpy(resp.response_buffer + hl, msg, ml);
                return resp;
            }
            total_written += bytes_written;
        }

        close(fd);
        if (flock)
            writer_unlock(flock);

        int status = file_existed ? 200 : 201;
        resp.status_code = status;
        const char *msg = (status == 200) ? "OK\n" : "Created\n";
        char header_out[256];
        size_t ml = strlen(msg);
        snprintf(header_out, sizeof(header_out), "HTTP/1.1 %d %s\r\nContent-Length: %zu\r\n\r\n",
            status, (status == 200 ? "OK" : "Created"), ml);
        size_t hl = strlen(header_out);
        resp.response_length = hl + ml;
        resp.response_buffer = malloc(resp.response_length);
        memcpy(resp.response_buffer, header_out, hl);
        memcpy(resp.response_buffer + hl, msg, ml);
    } else {
        // Not implemented
        resp.status_code = 501;
        const char *msg = "Not Implemented\n";
        char header_out[256];
        size_t ml = strlen(msg);
        snprintf(header_out, sizeof(header_out),
            "HTTP/1.1 501 Not Implemented\r\nContent-Length: %zu\r\n\r\n", ml);
        size_t hl = strlen(header_out);
        resp.response_length = hl + ml;
        resp.response_buffer = malloc(resp.response_length);
        memcpy(resp.response_buffer, header_out, hl);
        memcpy(resp.response_buffer + hl, msg, ml);
        return resp;
    }

    // If it's GET or PUT on a file, grab a commit order
    if ((strcmp(method, "GET") == 0 || strcmp(method, "PUT") == 0) && file_path[0] != '\0') {
        file_lock_node_t *node = get_file_lock_node(file_path);
        if (node) {
            pthread_mutex_lock(&node->commit_mutex);
            resp.commit_order = node->next_commit;
            node->next_commit++;
            pthread_mutex_unlock(&node->commit_mutex);
            resp.commit_node = node;
        }
    }

    return resp;
}

/* --- Worker Thread --- */
static void *worker_thread(void *arg) {
    (void) arg;
    while (1) {
        task_t *task = NULL;
        if (!queue_pop(task_queue, (void **) &task))
            continue;
        if (!task)
            continue;

        // Build the response (and spool file if GET)
        response_info_t resp = process_connection_ordered(task->client_socket);

        // If there's a commit node, wait until our turn
        if (resp.commit_node) {
            file_lock_node_t *node = resp.commit_node;
            pthread_mutex_lock(&node->commit_mutex);
            while (node->current_commit != resp.commit_order) {
                pthread_cond_wait(&node->commit_cond, &node->commit_mutex);
            }
            node->current_commit++;
            pthread_cond_broadcast(&node->commit_cond);
            pthread_mutex_unlock(&node->commit_mutex);
        }

        // 1) Send the response headers/body that are in resp.response_buffer
        write_all(resp.client_socket, resp.response_buffer, resp.response_length);

        // 2) If GET needs file streaming, open the temp file and send it now
        if (resp.need_file_stream && resp.temp_filename) {
            int fd = open(resp.temp_filename, O_RDONLY);
            if (fd >= 0) {
                char buf[FILE_BUFFER_SIZE];
                ssize_t bytes_read;
                while ((bytes_read = read(fd, buf, sizeof(buf))) > 0) {
                    write_all(resp.client_socket, buf, bytes_read);
                }
                close(fd);
            }
            unlink(resp.temp_filename);
            free(resp.temp_filename);
        }

        // Audit log
        pthread_mutex_lock(&audit_log_mutex);
        fprintf(stderr, "%s,%s,%d,%s\n", resp.method, resp.uri, resp.status_code, resp.request_id);
        fflush(stderr);
        pthread_mutex_unlock(&audit_log_mutex);

        close(resp.client_socket);
        free(resp.response_buffer);
        free(task);
    }
    return NULL;
}

/* --- Dispatcher Loop --- */
static void dispatcher_loop(void) {
    while (1) {
        int client_socket = ls_accept(global_socket);
        if (client_socket < 0) {
            fprintf(stdout, "Error accepting connection\n");
            continue;
        }
        task_t *task = malloc(sizeof(task_t));
        if (!task) {
            close(client_socket);
            continue;
        }
        task->client_socket = client_socket;
        task->order = 0;
        queue_push(task_queue, task);
    }
}

/* --- Signal Handler --- */
static void handle_sigint(int sig) {
    if (sig == SIGINT) {
        fprintf(stdout, "SIGINT received. Shutting down.\n");
        if (global_socket)
            ls_delete(&global_socket);
        exit(0);
    }
}

/* --- main --- */
int main(int argc, char **argv) {
    int port;
    int num_threads = 4; // default
    int opt;
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
        case 't': num_threads = atoi(optarg); break;
        default: fprintf(stdout, "Usage: %s [-t threads] <port>\n", argv[0]); exit(1);
        }
    }
    if (optind >= argc) {
        fprintf(stdout, "Usage: %s [-t threads] <port>\n", argv[0]);
        exit(1);
    }
    port = atoi(argv[optind]);
    if (port < 1 || port > 65535) {
        fprintf(stdout, "Invalid port number.\n");
        exit(1);
    }

    struct sigaction sa;
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);

    global_socket = ls_new(port);
    if (!global_socket) {
        fprintf(stdout, "Failed to create listener socket.\n");
        exit(1);
    }

    // Create the task queue
    task_queue = queue_new(1024);

    // Create worker threads
    pthread_t *workers = malloc(num_threads * sizeof(pthread_t));
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }

    fprintf(stdout, "Server running on port %d with %d worker threads.\n", port, num_threads);

    // Start dispatcher
    dispatcher_loop();

    // Cleanup (unreached in normal usage)
    ls_delete(&global_socket);
    free(workers);
    return 0;
}
