#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include <ctype.h>
#include <sys/stat.h>
#include <signal.h>

#include "iowrapper.h"
#include "listener_socket.h"
#include "protocol.h"

/* --- Macros --- */
#define MAX_URI_LENGTH    1024
#define MAX_METHOD_LENGTH 16
#define MAX_REQUEST_SIZE  2048
#define BUFFER_SIZE       4096
#define RESP_BUFF_SIZE    1024
#define FILE_BUFFER_SIZE  4096

/* --- Global Variables --- */
Listener_Socket_t *global_socket = NULL;

/* --- Utility Functions --- */

/* write_all: repeatedly calls write() until all count bytes are sent */
ssize_t write_all(int fd, const char *buf, size_t count) {
    ssize_t total_written = 0;
    while (total_written < (ssize_t) count) {
        ssize_t written = write(fd, buf + total_written, count - total_written);
        if (written <= 0)
            return -1;
        total_written += written;
    }
    return total_written;
}

/* send_response:
   Builds and sends a complete HTTP response.
   For GET responses with status 200 and a non-NULL file_path, the file is streamed.
   Note: This version does not close the client socket—process_connection does that. */
void send_response(int client_socket, int status_code, const char *file_path) {
    char response[RESP_BUFF_SIZE];
    const char *status_msg;
    const char *msg_body = NULL;
    size_t content_length = 0;

    switch (status_code) {
    case 200:
        status_msg = "OK";
        msg_body = "OK\n";
        break;
    case 201:
        status_msg = "Created";
        msg_body = "Created\n";
        break;
    case 400:
        status_msg = "Bad Request";
        msg_body = "Bad Request\n";
        break;
    case 403:
        status_msg = "Forbidden";
        msg_body = "Forbidden\n";
        break;
    case 404:
        status_msg = "Not Found";
        msg_body = "Not Found\n";
        break;
    case 500:
        status_msg = "Internal Server Error";
        msg_body = "Internal Server Error\n";
        break;
    case 501:
        status_msg = "Not Implemented";
        msg_body = "Not Implemented\n";
        break;
    case 505:
        status_msg = "Version Not Supported";
        msg_body = "Version Not Supported\n";
        break;
    default:
        status_msg = "Unknown Error";
        msg_body = "Unknown Error\n";
        break;
    }

    /* For a GET request that successfully found a file, stream its contents. */
    if (status_code == 200 && file_path != NULL) {
        int fd = open(file_path, O_RDONLY);
        if (fd < 0) {
            /* If file cannot be opened, respond with 404 */
            send_response(client_socket, 404, NULL);
            return;
        }
        struct stat file_stat;
        if (fstat(fd, &file_stat) < 0) {
            close(fd);
            send_response(client_socket, 500, NULL);
            return;
        }
        content_length = file_stat.st_size;
        snprintf(response, sizeof(response),
            "HTTP/1.1 200 OK\r\n"
            "Content-Length: %zu\r\n"
            "\r\n",
            content_length);
        write_all(client_socket, response, strlen(response));

        /* Send the file contents in chunks */
        char file_buffer[FILE_BUFFER_SIZE];
        ssize_t bytes_read;
        while ((bytes_read = read(fd, file_buffer, FILE_BUFFER_SIZE)) > 0) {
            if (write_all(client_socket, file_buffer, bytes_read) < 0) {
                break;
            }
        }
        close(fd);
        return;
    }

    /* For error responses or other cases */
    content_length = strlen(msg_body);
    snprintf(response, sizeof(response),
        "HTTP/1.1 %d %s\r\n"
        "Content-Length: %zu\r\n"
        "\r\n%s",
        status_code, status_msg, content_length, msg_body);
    write_all(client_socket, response, strlen(response));
}

/* valid_header_key: checks that a header key starts with a letter and that every
   character is alphanumeric, a hyphen, or a period */
bool valid_header_key(const char *key) {
    if (!key || !isalpha((unsigned char) key[0]))
        return false;
    for (int i = 0; key[i] != '\0'; i++) {
        char c = key[i];
        if (!isalnum((unsigned char) c) && c != '-' && c != '.')
            return false;
    }
    return true;
}

/* parse_http_request:
   Parses the header portion of an HTTP request.
   Fills in method_out, uri_out, and sets *content_len if provided.
   Returns 0 on success or an HTTP error code (e.g. 400, 501, 505) on failure. */
int parse_http_request(const char *header, char *method_out, char *uri_out, int *content_len) {
    *content_len = -1;
    /* Ensure that the header contains the required "\r\n\r\n" sequence */
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

    /* Validate method format: must be all uppercase letters */
    for (int i = 0; req_method[i]; i++) {
        if (req_method[i] < 'A' || req_method[i] > 'Z') {
            free(req_copy);
            return 400;
        }
    }
    /* If method is not GET or PUT, it is unsupported */
    if (strcmp(req_method, "GET") != 0 && strcmp(req_method, "PUT") != 0) {
        free(req_copy);
        return 501;
    }
    /* Validate HTTP version */
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

    /* Copy validated method and URI to outputs */
    strncpy(method_out, req_method, MAX_METHOD_LENGTH - 1);
    strncpy(uri_out, req_uri, MAX_URI_LENGTH - 1);

    int has_content_length = 0;
    /* Process remaining header lines */
    while ((line = strtok_r(NULL, "\r\n", &saveptr)) != NULL) {
        if (line[0] == '\0')
            break; // empty line indicates end of headers

        char *colon = strchr(line, ':');
        if (!colon) {
            free(req_copy);
            return 400; // invalid header: missing colon
        }
        *colon = '\0';
        char *key = line;
        char *value = colon + 1;

        /* Trim leading whitespace in key and value */
        while (*key && isspace((unsigned char) *key))
            key++;
        while (*value && isspace((unsigned char) *value))
            value++;

        if (!valid_header_key(key)) {
            free(req_copy);
            return 400;
        }

        /* Look for Content-Length header (case-insensitive) */
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

/* --- Request Handlers --- */

/* process_GET_request:
   Checks that the file exists, is not a directory, and is readable before sending it. */
void process_GET_request(int client_socket, const char *uri) {
    char file_path[128];
    snprintf(file_path, sizeof(file_path), ".%s", uri);

    struct stat path_stat;
    if (stat(file_path, &path_stat) != 0) {
        send_response(client_socket, 404, NULL);
        return;
    }
    if (S_ISDIR(path_stat.st_mode) || access(file_path, R_OK) != 0) {
        send_response(client_socket, 403, NULL);
        return;
    }
    send_response(client_socket, 200, file_path);
}

/* process_PUT_request:
   Writes any already received request body then reads remaining data from the socket.
   Uses safe path handling and returns 200 (if overwritten) or 201 (if new file created). */
void process_PUT_request(int client_socket, const char *uri, size_t content_length,
    char *initial_body, size_t initial_body_size) {
    if (content_length <= 0) {
        send_response(client_socket, 400, NULL);
        return;
    }
    char file_path[128];
    snprintf(file_path, sizeof(file_path), ".%s", uri);

    struct stat file_stat;
    bool file_existed = (stat(file_path, &file_stat) == 0 && S_ISREG(file_stat.st_mode));

    /* Reject if target is a directory */
    if (stat(file_path, &file_stat) == 0 && S_ISDIR(file_stat.st_mode)) {
        send_response(client_socket, 403, NULL);
        return;
    }

    /* Check write permissions if file exists */
    if (file_existed && access(file_path, W_OK) == -1) {
        send_response(client_socket, 403, NULL);
        return;
    }

    int fd = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        if (errno == EACCES)
            send_response(client_socket, 403, NULL);
        else if (errno == ENOENT)
            send_response(client_socket, 404, NULL);
        else
            send_response(client_socket, 500, NULL);
        return;
    }

    ssize_t total_written = 0;
    /* Write any initial body data already read */
    if (initial_body_size > 0) {
        ssize_t written = write_n_bytes(fd, initial_body, initial_body_size);
        if (written != (ssize_t) initial_body_size) {
            close(fd);
            send_response(client_socket, 500, NULL);
            return;
        }
        total_written += written;
    }

    /* Continue reading until the full Content-Length is received */
    char buffer[BUFFER_SIZE];
    while (total_written < (ssize_t) content_length) {
        size_t to_read = (content_length - total_written < BUFFER_SIZE)
                             ? (content_length - total_written)
                             : BUFFER_SIZE;
        ssize_t bytes_read = read_n_bytes(client_socket, buffer, to_read);
        if (bytes_read <= 0) {
            close(fd);
            send_response(client_socket, 500, NULL);
            return;
        }
        ssize_t bytes_written = write_n_bytes(fd, buffer, bytes_read);
        if (bytes_written != bytes_read) {
            close(fd);
            send_response(client_socket, 500, NULL);
            return;
        }
        total_written += bytes_written;
    }
    close(fd);
    send_response(client_socket, file_existed ? 200 : 201, NULL);
}

/* --- Connection Handling --- */

/*
   process_connection:
   Reads the full HTTP request (header and any initial body data),
   validates it, and then dispatches to the appropriate GET or PUT handler.
*/
void process_connection(int client_socket) {
    char request_buf[MAX_REQUEST_SIZE];
    ssize_t total_read = 0;

    /* Read data until we have the full header (i.e. "\r\n\r\n" is seen) */
    while (total_read < MAX_REQUEST_SIZE - 1) {
        ssize_t bytes
            = read(client_socket, request_buf + total_read, MAX_REQUEST_SIZE - total_read - 1);
        if (bytes < 0) {
            send_response(client_socket, 400, NULL);
            close(client_socket);
            return;
        }
        if (bytes == 0)
            break; // connection closed by client
        total_read += bytes;
        request_buf[total_read] = '\0';
        if (strstr(request_buf, "\r\n\r\n") != NULL)
            break;
    }
    if (strstr(request_buf, "\r\n\r\n") == NULL) {
        send_response(client_socket, 400, NULL);
        close(client_socket);
        return;
    }

    /* Separate the header from any initial body data */
    char *header_end = strstr(request_buf, "\r\n\r\n");
    size_t header_length = header_end - request_buf + 4;
    if (header_length >= sizeof(request_buf))
        header_length = sizeof(request_buf) - 1;
    char header_buf[MAX_REQUEST_SIZE];
    memcpy(header_buf, request_buf, header_length);
    header_buf[header_length] = '\0';

    char method[MAX_METHOD_LENGTH] = { 0 };
    char uri[MAX_URI_LENGTH] = { 0 };
    int content_length = 0;
    int parse_result = parse_http_request(header_buf, method, uri, &content_length);
    if (parse_result != 0) {
        send_response(client_socket, parse_result, NULL);
        close(client_socket);
        return;
    }

    size_t initial_body_size = total_read - header_length;
    char *initial_body = request_buf + header_length;

    if (strcmp(method, "PUT") == 0) {
        process_PUT_request(client_socket, uri, content_length, initial_body, initial_body_size);
    } else if (strcmp(method, "GET") == 0) {
        process_GET_request(client_socket, uri);
    } else {
        send_response(client_socket, 501, NULL);
    }
    close(client_socket);
}

/* --- Signal Handling --- */
void handle_sigint(int sig) {
    if (sig == SIGINT) {
        printf("SIGINT received. Cleaning up listener socket.\n");
        if (global_socket)
            ls_delete(&global_socket);
        printf("Server shutting down gracefully.\n");
        exit(0);
    }
}

/* --- Main --- */
int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    int port = atoi(argv[1]);
    if (port < 1 || port > 65535) {
        fprintf(stderr, "Invalid port number.\n");
        exit(1);
    }

    /* Set up SIGINT handler for graceful shutdown */
    struct sigaction sa;
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);

    global_socket = ls_new(port);
    if (!global_socket) {
        fprintf(stderr, "Failed to create listener socket.\n");
        exit(1);
    }
    printf("Server running on port %d. Press Ctrl+C to stop.\n", port);

    while (1) {
        int client_socket = ls_accept(global_socket);
        if (client_socket < 0) {
            fprintf(stderr, "Error accepting connection\n");
            continue;
        }
        process_connection(client_socket);
    }
    ls_delete(&global_socket);
    return 0;
}
