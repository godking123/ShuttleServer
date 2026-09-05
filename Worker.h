#ifndef WORKER_H
#define WORKER_H

#include <string>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <vector>

enum class WorkerStatus : uint8_t {
    Idle,
    Busy,
    Dead
};

struct Worker {
    std::string id;
    WorkerStatus status;
    std::chrono::steady_clock::time_point lastHeartbeat;
    std::string currentJobId;
    std::string ip;
    int port;

    Worker(const std::string& workerId, const std::string& workerIp, int workerPort)
        : id(workerId), status(WorkerStatus::Idle),
          lastHeartbeat(std::chrono::steady_clock::now()),
          currentJobId(""), ip(workerIp), port(workerPort)
    {}
};

class WorkerRegistry {
private:
    std::unordered_map<std::string, Worker> workers;
    std::mutex mtx;

public:
   void registerWorker(const std::string& id, const std::string& ip, int port) {
        std::lock_guard<std::mutex> lk(mtx);
        workers.emplace(id, Worker(id, ip, port));
   }

    void updateHeartbeat(const std::string& id) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = workers.find(id);
        if (it != workers.end()) {
            it->second.lastHeartbeat = std::chrono::steady_clock::now();
            if (it->second.status == WorkerStatus::Dead) {
                it->second.status = WorkerStatus::Idle; // worker came back
            }
        }
    }

    void setStatus(const std::string& id, WorkerStatus status) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = workers.find(id);
        if (it != workers.end()) {
            it->second.status = status;
        }
    }

    void setCurrentJob(const std::string& id, const std::string& jobId) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = workers.find(id);
        if (it != workers.end()) {
            it->second.currentJobId = jobId;
        }
    }

    std::string getCurrentJob(const std::string& id) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = workers.find(id);
        return (it != workers.end()) ? it->second.currentJobId : "";
    }

    std::vector<std::string> checkTimeouts(int timeoutMs) {
        std::lock_guard<std::mutex> lk(mtx);
        std::vector<std::string> newlyDead;
        auto now = std::chrono::steady_clock::now();

        for (auto& pair : workers) {
            Worker& w = pair.second;
            if (w.status == WorkerStatus::Dead) continue;

            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - w.lastHeartbeat).count();
            if (elapsed > timeoutMs) {
                w.status = WorkerStatus::Dead;
                newlyDead.push_back(w.id);
            }
        }
        return newlyDead;
    }

    std::string findIdleWorker() {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto& pair : workers) {
            if (pair.second.status == WorkerStatus::Idle) return pair.first;
        }
        return "";
    }

    bool getAddress(const std::string& id, std::string& ipOut, int& portOut) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = workers.find(id);
        if (it == workers.end()) return false;
        ipOut = it->second.ip;
        portOut = it->second.port;
        return true;
    }
};

#endif // WORKER_H
