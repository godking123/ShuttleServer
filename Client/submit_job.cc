#include <iostream>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "../Protocol.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: ./submit_job \"<command>\"\n";
        return 1;
    }

    const char* piIp = std::getenv("PI_IP");
    if (piIp == nullptr) {
        std::cerr << "PI_IP environment variable not set\n";
        return 1;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket failed");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(5000);
    if (inet_pton(AF_INET, piIp, &addr.sin_addr) <= 0) {
        perror("invalid address");
        close(sock);
        return 1;
    }

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect failed");
        close(sock);
        return 1;
    }

    std::string msg = MsgType::JOB + "|" + argv[1] + "\n";
    send(sock, msg.c_str(), msg.length(), 0);
    close(sock);

    std::cout << "Job submitted.\n";
    return 0;
}
