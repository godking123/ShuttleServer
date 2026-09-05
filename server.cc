#include <iostream>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "ThreadPool.h"
#include "Protocol.h"
#include "Worker.h"
#include "Job.h"
#include <atomic>

WorkerRegistry registry;
JobRegistry jobRegistry;

std::atomic<int> nextJobId{1};

void handleClient(int clientSocket, const std::string& clientIp) {
    std::string line = readLine(clientSocket);
    if (!line.empty()) {
        std::vector<std::string> fields = splitMessage(line);

        if (fields[0] == MsgType::REGISTER && fields.size() >= 3) {
            int port = std::stoi(fields[2]);
            registry.registerWorker(fields[1], clientIp, port);
            std::cout << "Registered worker: " << fields[1] << " at " << clientIp << ":" << port << "\n";
        } else if (fields[0] == MsgType::PING && fields.size() >= 2) {
            registry.updateHeartbeat(fields[1]);
            std::cout << "Heartbeat from: " << fields[1] << "\n";

        } else if (fields[0] == MsgType::JOB && fields.size() >= 2) {
            std::string jobId = std::to_string(nextJobId++);
            jobRegistry.addJob(jobId, fields[1]);
            std::cout << "Job submitted: " << jobId << " -> " << fields[1] << "\n";
        }
    }

    close(clientSocket);
}

void heartbeatMonitor(int intervalMs, int timeoutMs) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));

        std::vector<std::string> deadWorkers = registry.checkTimeouts(timeoutMs);
        for (const std::string& workerId : deadWorkers) {
            std::cout << "Worker " << workerId << " marked DEAD (missed heartbeat timeout)\n";

            std::string jobId = registry.getCurrentJob(workerId);
            if (!jobId.empty()) {
                std::cout << "  Rescheduling job " << jobId << " (was on " << workerId << ")\n";
                jobRegistry.requeueJob(jobId);
            }
        }
    }
}

void dispatcher() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));

        std::string workerId = registry.findIdleWorker();
        std::string jobId = jobRegistry.findPendingJob();
        if (workerId.empty() || jobId.empty()) continue;

        std::string workerIp;
        int workerPort;
        if (!registry.getAddress(workerId, workerIp, workerPort)) continue;

        std::string payload = jobRegistry.getPayload(jobId);
        jobRegistry.assignJob(jobId, workerId);
        registry.setCurrentJob(workerId, jobId);
        registry.setStatus(workerId, WorkerStatus::Busy);

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(workerPort);
        inet_pton(AF_INET, workerIp.c_str(), &addr.sin_addr);

        if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("dispatch connect failed");
            close(sock);
            jobRegistry.requeueJob(jobId);
            registry.setStatus(workerId, WorkerStatus::Idle);
            continue;
        }

        std::string msg = buildJobMessage(jobId, payload);
        send(sock, msg.c_str(), msg.length(), 0);
        std::cout << "Dispatched job " << jobId << " to " << workerId << "\n";

        std::string resultLine = readLine(sock);
        close(sock);

        if (!resultLine.empty()) {
            std::vector<std::string> fields = splitMessage(resultLine);
            if (fields[0] == MsgType::RESULT && fields.size() >= 3) {
                std::cout << "Job " << fields[1] << " result: " << fields[2] << "\n";
                jobRegistry.completeJob(fields[1]);
                registry.setStatus(workerId, WorkerStatus::Idle);
            }
        }
    }
}

int main() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        perror("socket failed");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(5000);

    if (bind(serverSocket, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        return 1;
    }

    if (listen(serverSocket, 5) < 0) {
        perror("listen failed");
        return 1;
    }

    std::cout << "Listening on port 5000...\n";

    ThreadPool pool(4);

    std::thread monitorThread(heartbeatMonitor, 5000, 15000);
    monitorThread.detach();

    std::thread dispatcherThread(dispatcher);
    dispatcherThread.detach();

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientSocket = accept(serverSocket, (sockaddr*)&clientAddr, &clientLen);
        if (clientSocket < 0) { perror("accept failed"); continue; }

        char ipBuf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddr.sin_addr, ipBuf, sizeof(ipBuf));
        std::string clientIp(ipBuf);

        pool.submit(handleClient, clientSocket, clientIp);
    }

    close(serverSocket);
    return 0;
}
