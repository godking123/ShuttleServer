#include <iostream>
#include <cstring>
#include <cstdlib>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "../Protocol.h"

const std::string WORKER_ID = "worker1";
const int LISTEN_PORT = 6000; // this worker's own listening port

bool sendOnce(const char* piIp, const std::string& msg) {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket < 0) { perror("socket failed"); return false; }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(5000);

    if (inet_pton(AF_INET, piIp, &serverAddr.sin_addr) <= 0) {
        perror("invalid address");
        close(clientSocket);
        return false;
    }

    if (connect(clientSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("connect failed");
        close(clientSocket);
        return false;
    }

    send(clientSocket, msg.c_str(), msg.length(), 0);
    close(clientSocket);
    return true;
}

void heartbeatLoop(const char* piIp) {
    // one-time registration, includes this worker's listen port
    if (sendOnce(piIp, buildRegisterMessage(WORKER_ID, LISTEN_PORT))) {
        std::cout << "Registered as " << WORKER_ID << " on port " << LISTEN_PORT << "\n";
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (sendOnce(piIp, buildPing(WORKER_ID))) {
            std::cout << "Heartbeat sent\n";
        }
    }
}

void handleIncomingJob(int connSocket) {
    std::string line = readLine(connSocket);
    if (!line.empty()) {
        std::vector<std::string> fields = splitMessage(line);
        if (fields[0] == MsgType::JOB && fields.size() >= 3) {
            std::string jobId = fields[1];
            std::string payload = fields[2];
            std::cout << "Received job " << jobId << ": " << payload << "\n";

            // fake execution for now — real popen() swap comes later
            std::this_thread::sleep_for(std::chrono::seconds(3));

            std::string result = buildResultMessage(jobId, "success");
            send(connSocket, result.c_str(), result.length(), 0);
            std::cout << "Job " << jobId << " done, result sent\n";
        }
    }
    close(connSocket);
}

void jobListener() {
    int listenSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (listenSocket < 0) { perror("socket failed"); return; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(LISTEN_PORT);

    if (bind(listenSocket, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        return;
    }
    if (listen(listenSocket, 5) < 0) {
        perror("listen failed");
        return;
    }

    std::cout << "Worker listening for jobs on port " << LISTEN_PORT << "...\n";

    while (true) {
        int connSocket = accept(listenSocket, nullptr, nullptr);
        if (connSocket < 0) { perror("accept failed"); continue; }
        std::thread(handleIncomingJob, connSocket).detach();
    }
}

int main() {
    const char* piIp = std::getenv("PI_IP");
    if (piIp == nullptr) {
        std::cerr << "PI_IP environment variable not set\n";
        return 1;
    }

    std::thread listenerThread(jobListener);
    std::thread heartbeatThread(heartbeatLoop, piIp);

    listenerThread.join();
    heartbeatThread.join();

    return 0;
}
