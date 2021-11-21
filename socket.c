#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>

enum req_type {
    
}

struct Request {
    
}

int main() {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0); // TCP
    if (socket_fd < 0) {
        perror("socket failed");
        return 1;
    }
    
    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    srandomdev();
    int port = random()%32768 + 32768;
    server.sin_port = htons(port);
    printf("port is %d\n", port);
    
    int bind_resp = bind(socket_fd, (struct sockaddr *)&server, sizeof(server));
    if (bind_resp < 0) {
        perror("Bind failed");
        return 1;
    }
    
    if (listen(socket_fd, 3)) {
        perror("listen");
        return 1;
    }
    
    unsigned int size = sizeof(server);
    int client_sock = accept(socket_fd, (struct sockaddr *)&server, &size);
    char *buf = malloc(4096);
    ssize_t length = recv(client_sock, &buf, 4096, 0);
    printf("buf is %s\n", buf);
    
    send(client_sock, buf, 5, 0);
}
