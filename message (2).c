#include "client_socket.h"
#include "iowrapper.h"
#include "listener_socket.h"
#include "prequest.h"
#include "a5protocol.h"

#include <assert.h>
#include <err.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <regex.h>

#ifndef MAX_KEY_LEN
#define MAX_KEY_LEN 512
#endif

// Writes all bytes to fd, handling partial writes/EINTR.
ssize_t write_all(int fd, const void *buffer, size_t count) {
    size_t total_written = 0;
    const char *buf = (const char *) buffer;
    while (total_written < count) {
        ssize_t written = write(fd, buf + total_written, count - total_written);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "write_all: error: %s\n", strerror(errno));
            return -1;
        }
        total_written += written;
    }
    return total_written;
}

typedef struct cache_entry {
    char key[MAX_KEY_LEN];
    char *response;
    size_t response_size;
    struct cache_entry *prev;
    struct cache_entry *next;
} cache_entry_t;

typedef struct cache {
    cache_entry_t *head;
    cache_entry_t *tail;
    int count;
    int capacity;
    char mode[5];
} cache_t;

static cache_t *global_cache = NULL;

// -------------- Cache Logic --------------
static void remove_cache_entry(cache_entry_t *entry);

static void cache_init(int capacity, const char *mode) {
    fprintf(stderr, "DEBUG: cache_init(capacity=%d, mode=%s)\n", capacity, mode);
    global_cache = malloc(sizeof(cache_t));
    if (!global_cache) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    global_cache->head = NULL;
    global_cache->tail = NULL;
    global_cache->count = 0;
    global_cache->capacity = capacity;
    strncpy(global_cache->mode, mode, sizeof(global_cache->mode));
    global_cache->mode[sizeof(global_cache->mode) - 1] = '\0';
}

static cache_entry_t *cache_lookup(const char *key) {
    if (!global_cache)
        return NULL;
    cache_entry_t *curr = global_cache->head;
    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            fprintf(stderr, "DEBUG: cache_lookup: HIT %s\n", key);
            return curr;
        }
        curr = curr->next;
    }
    fprintf(stderr, "DEBUG: cache_lookup: MISS %s\n", key);
    return NULL;
}

// Evict one entry based on the chosen policy.

static void cache_evict(void) {
    if (!global_cache)
        return;
    if (global_cache->count < global_cache->capacity)
        return;
    // Evict an entry if count == capacity.
    if (strcmp(global_cache->mode, "FIFO") == 0) {
        fprintf(stderr, "DEBUG: Evicting FIFO: key=%s\n",
            global_cache->head ? global_cache->head->key : "(null)");
        remove_cache_entry(global_cache->head);
    } else {
        // LRU
        fprintf(stderr, "DEBUG: Evicting LRU: key=%s\n",
            global_cache->tail ? global_cache->tail->key : "(null)");
        remove_cache_entry(global_cache->tail);
    }
}

static void remove_cache_entry(cache_entry_t *entry) {
    if (!entry)
        return;
    fprintf(stderr, "DEBUG: remove_cache_entry: key=%s\n", entry->key);
    if (entry->prev)
        entry->prev->next = entry->next;
    else
        global_cache->head = entry->next;
    if (entry->next)
        entry->next->prev = entry->prev;
    else
        global_cache->tail = entry->prev;
    free(entry->response);
    free(entry);
    global_cache->count--;
}

// Insert a new entry at the head (LRU) or tail (FIFO).
static void cache_insert(const char *key, const char *resp, size_t size) {
    if (!global_cache)
        return;
    if (size > MAX_CACHE_ENTRY) {
        // Don’t cache items >1MB, but still pass them through.
        fprintf(stderr, "DEBUG: Not caching %s (size=%zu > 1MB)\n", key, size);
        return;
    }
    if (global_cache->count >= global_cache->capacity) {
        cache_evict(); // Evict exactly one entry before insertion
    }
    cache_entry_t *new_ent = malloc(sizeof(cache_entry_t));
    if (!new_ent) {
        perror("malloc");
        return;
    }
    strncpy(new_ent->key, key, MAX_KEY_LEN);
    new_ent->key[MAX_KEY_LEN - 1] = '\0';
    new_ent->response = malloc(size);
    if (!new_ent->response) {
        perror("malloc");
        free(new_ent);
        return;
    }
    memcpy(new_ent->response, resp, size);
    new_ent->response_size = size;
    new_ent->prev = NULL;
    new_ent->next = NULL;
    // Insert depending on mode.
    if (strcmp(global_cache->mode, "FIFO") == 0) {
        // FIFO: insert at tail
        new_ent->prev = global_cache->tail;
        if (global_cache->tail) {
            global_cache->tail->next = new_ent;
        }
        global_cache->tail = new_ent;
        if (!global_cache->head) {
            global_cache->head = new_ent;
        }
    } else {
        // LRU: insert at head
        new_ent->next = global_cache->head;
        if (global_cache->head) {
            global_cache->head->prev = new_ent;
        }
        global_cache->head = new_ent;
        if (!global_cache->tail) {
            global_cache->tail = new_ent;
        }
    }
    global_cache->count++;
    fprintf(
        stderr, "DEBUG: cache_insert: key=%s, new count=%d\n", new_ent->key, global_cache->count);
}

// Move an entry to the head for LRU.
static void update_lru(cache_entry_t *entry) {
    if (!global_cache)
        return;
    if (strcmp(global_cache->mode, "LRU") != 0)
        return;
    if (!entry || entry == global_cache->head)
        return;
    fprintf(stderr, "DEBUG: update_lru: moving key=%s to head\n", entry->key);
    // Unlink
    if (entry->prev)
        entry->prev->next = entry->next;
    if (entry->next)
        entry->next->prev = entry->prev;
    if (entry == global_cache->tail)
        global_cache->tail = entry->prev;
    // Insert at head
    entry->prev = NULL;
    entry->next = global_cache->head;
    if (global_cache->head)
        global_cache->head->prev = entry;
    global_cache->head = entry;
}

// Free entire cache
static void free_cache(void) {
    if (!global_cache)
        return;
    fprintf(stderr, "DEBUG: free_cache\n");
    cache_entry_t *curr = global_cache->head;
    while (curr) {
        cache_entry_t *next = curr->next;
        free(curr->response);
        free(curr);
        curr = next;
    }
    free(global_cache);
    global_cache = NULL;
}

// -------------- Reading Full Server Response --------------
// Remove artificial limit so we always forward the entire response to the client.
static char *read_server_response(int serverfd, size_t *response_size) {
    size_t capacity = 8192;
    char *buffer = malloc(capacity);
    if (!buffer) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    size_t total = 0;
    while (1) {
        ssize_t n = read(serverfd, buffer + total, capacity - total);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "DEBUG: read_server_response: error: %s\n", strerror(errno));
            break;
        }
        if (n == 0) {
            // Server closed connection
            break;
        }
        total += n;
        if (total == capacity) {
            size_t new_cap = capacity * 2;
            char *tmp = realloc(buffer, new_cap);
            if (!tmp) {
                perror("realloc");
                break;
            }
            buffer = tmp;
            capacity = new_cap;
        }
    }
    *response_size = total;
    fprintf(stderr, "DEBUG: read_server_response: total=%zu bytes\n", total);
    return buffer;
}

char *modify_response_for_cache(const char *response, size_t orig_size, size_t *new_size_out) {
    // Find end of header block.
    const char *header_end = strstr(response, "\r\n\r\n");
    if (!header_end) {
        // No header block found; just return a copy.
        char *copy = malloc(orig_size);
        if (copy)
            memcpy(copy, response, orig_size);
        *new_size_out = orig_size;
        return copy;
    }
    size_t header_len = header_end - response;
    size_t body_len = orig_size - (header_len + 4); // skip the "\r\n\r\n"
    // Copy header block into a modifiable buffer.
    char *headers = malloc(header_len + 1);
    if (!headers) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    memcpy(headers, response, header_len);
    headers[header_len] = '\0';
    // We'll rebuild the headers in a new buffer.
    char new_headers[4096]; // Assume headers will not exceed 4096 bytes.
    new_headers[0] = '\0';
    char *saveptr;
    char *line = strtok_r(headers, "\r\n", &saveptr);
    while (line) {
        // Check if this line is a Content-Length header.
        if (strncasecmp(line, "Content-Length:", 15) == 0) {
            // Parse the old content length.
            int old_length = atoi(line + 15);
            // Our inserted header: "Cached: True\r\n"
            int extra = strlen("Cached: True\r\n");
            int new_length = old_length + extra;
            char new_line[100];
            snprintf(new_line, sizeof(new_line), "Content-Length: %d", new_length);
            strcat(new_headers, new_line);
        } else {
            strcat(new_headers, line);
        }
        strcat(new_headers, "\r\n");
        line = strtok_r(NULL, "\r\n", &saveptr);
    }
    free(headers);
    // Now insert our "Cached: True" header at the end of the header block.
    strcat(new_headers, "Cached: True\r\n");
    // Terminate the header block.
    strcat(new_headers, "\r\n");
    size_t new_header_len = strlen(new_headers);
    size_t new_total = new_header_len + body_len;
    char *new_resp = malloc(new_total);
    if (!new_resp) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    // Copy the modified header.
    memcpy(new_resp, new_headers, new_header_len);
    // Append the original body (unchanged).
    memcpy(new_resp + new_header_len, response + header_len + 4, body_len);
    *new_size_out = new_total;
    return new_resp;
}

// Insert "Cached: True" before the blank line that ends the headers.
char *add_cached_header(const char *response, size_t orig_size, size_t *new_size_out) {
    // Look for the end of headers
    const char *headers_end = strstr(response, "\r\n\r\n");
    if (!headers_end) {
        // No header boundary found; just return a copy.
        // The new size is the same as the old size.
        char *copy = malloc(orig_size);
        if (copy)
            memcpy(copy, response, orig_size);
        *new_size_out = orig_size;
        return copy;
    }
    size_t offset = headers_end - response; // position of "\r\n\r\n"
    const char *insertion = CACHED_HEADER "\r\n"; // "Cached: True\r\n"
    size_t insertion_len = strlen(insertion);
    // The new total size after inserting the header line
    size_t new_size = orig_size + insertion_len;
    char *new_resp = malloc(new_size);
    if (!new_resp) {
        *new_size_out = 0;
        return NULL;
    }
    // Copy up to the header boundary
    memcpy(new_resp, response, offset);
    // Insert "Cached: True\r\n"
    memcpy(new_resp + offset, insertion, insertion_len);
    // Copy the remainder of the response (including the "\r\n\r\n" and body)
    memcpy(new_resp + offset + insertion_len, response + offset, orig_size - offset);
    *new_size_out = new_size;
    return new_resp;
}

// -------------- Connection Handler --------------

static void handle_connection(uintptr_t connfd) {
    fprintf(stderr, "DEBUG: handle_connection: fd=%lu\n", connfd);
    Prequest_t *preq = prequest_new(connfd);
    if (!preq) {
        fprintf(stderr, "DEBUG: Bad request on fd=%lu\n", connfd);
        close(connfd);
        return;
    }

    char *host = prequest_get_host(preq);
    char *orig_uri = prequest_get_uri(preq);
    char *local_uri = strdup(orig_uri);
    int32_t server_port = prequest_get_port(preq);
    fprintf(stderr, "DEBUG: parsed request => host=%s, port=%d, uri=%s\n", host, server_port,
        local_uri);
    // Ensure the URI starts with '/'
    if (local_uri && local_uri[0] != '/') {
        fprintf(stderr, "DEBUG: URI missing leading '/', fixing it.\n");
        size_t old_len = strlen(local_uri);
        char *fixed_uri = malloc(old_len + 2); // 1 slash + old_len + 1 for '\0'
        if (fixed_uri) {
            fixed_uri[0] = '/';
            strcpy(fixed_uri + 1, local_uri);
            free(local_uri);
            local_uri = fixed_uri; // We won't free the old pointer if it's managed by prequest
        }
    }
    // Build a cache key => "host:port/uri"
    char key[MAX_KEY_LEN];
    snprintf(key, sizeof(key), "%s:%d%s", host, server_port, local_uri);
    // Check cache
    cache_entry_t *entry = cache_lookup(key);
    if (entry) {
        // Cache hit
        if (strcmp(global_cache->mode, "LRU") == 0) {
            update_lru(entry);
        }
        size_t modified_size = 0;
        char *modified_resp
            = modify_response_for_cache(entry->response, entry->response_size, &modified_size);
        if (modified_resp) {
            write_all(connfd, modified_resp, modified_size);
            free(modified_resp);
        } else {
            // fallback: send original
            write_all(connfd, entry->response, entry->response_size);
        }
        free(local_uri);
        prequest_delete(&preq);
        close(connfd);
        return;
    }
    // Cache miss or caching disabled
    int serverfd = cs_new(host, server_port);
    if (serverfd < 0) {
        fprintf(stderr, "DEBUG: cs_new failed for host=%s port=%d\n", host, server_port);
        free(local_uri);
        prequest_delete(&preq);
        close(connfd);
        return;
    }
    // Build a minimal HTTP/1.1 request
    char request_buf[2048];
    snprintf(request_buf, sizeof(request_buf),
        "GET %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "Connection: close\r\n"
        "\r\n",
        local_uri, host);
    fprintf(stderr, "DEBUG: sending to server:\n%s", request_buf);
    write_all(serverfd, request_buf, strlen(request_buf));
    size_t resp_size = 0;
    char *server_resp = read_server_response(serverfd, &resp_size);
    // Forward entire response to client
    write_all(connfd, server_resp, resp_size);
    // Cache if enabled and size <= 1MB
    if (global_cache && resp_size <= MAX_CACHE_ENTRY) {
        cache_insert(key, server_resp, resp_size);
    }
    free(server_resp);
    free(local_uri);
    prequest_delete(&preq);
    close(serverfd);
    close(connfd);
    fprintf(stderr, "DEBUG: handle_connection done for key=%s\n", key);
}

// -------------- Main --------------

static void usage(FILE *stream, const char *exec) {
    fprintf(stream, "usage: %s <port> <mode> <n>\n", exec);
    fprintf(stream, "  port in [1,65535]\n");
    fprintf(stream, "  mode in {FIFO, LRU}\n");
    fprintf(stream, "  n in [0,1024]\n");
}

int main(int argc, char **argv) {
    if (argc != 4) {
        usage(stderr, argv[0]);
        return EXIT_FAILURE;
    }
    int port = atoi(argv[1]);
    char *mode = argv[2];
    int cache_size = atoi(argv[3]);
    if (port < 1 || port > 65535 || (strcmp(mode, "FIFO") != 0 && strcmp(mode, "LRU") != 0)
        || cache_size < 0 || cache_size > 1024) {
        fprintf(stderr, "Invalid Argument\n");
        return EXIT_FAILURE;
    }
    if (cache_size > 0) {
        cache_init(cache_size, mode);
    } else {
        fprintf(stderr, "DEBUG: Caching disabled (n=0)\n");
    }
    Listener_Socket_t *lsock = ls_new(port);
    if (!lsock) {
        fprintf(stderr, "DEBUG: Failed to listen on port=%d\n", port);
        return EXIT_FAILURE;
    }
    fprintf(stderr, "DEBUG: Proxy listening on port=%d, mode=%s, capacity=%d\n", port, mode,
        cache_size);
    while (1) {
        fprintf(stderr, "DEBUG: Waiting for incoming connection...\n");
        uintptr_t connfd = ls_accept(lsock);
        if (connfd <= 0) {
            fprintf(stderr, "DEBUG: ls_accept error\n");
            continue;
        }
        handle_connection(connfd);
    }
    ls_delete(&lsock);
    free_cache();
    return EXIT_SUCCESS;
}
