#ifndef JOB_H
#define JOB_H

#include <string>
#include <unordered_map>
#include <mutex>

enum class JobStatus : uint8_t {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed
};

struct Job {
    std::string id;
    std::string payload;
    JobStatus status;
    std::string assignedWorkerId;

    Job(const std::string& jobId, const std::string& jobPayload)
        : id(jobId),
          payload(jobPayload),
          status(JobStatus::Pending),
          assignedWorkerId("")
    {}
};

class JobRegistry {
private:
    std::unordered_map<std::string, Job> jobs;
    std::mutex mtx;

public:
    void addJob(const std::string& id, const std::string& payload) {
        std::lock_guard<std::mutex> lk(mtx);
        jobs.emplace(id, Job(id, payload));
    }

    void assignJob(const std::string& jobId, const std::string& workerId) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = jobs.find(jobId);
        if (it != jobs.end()) {
            it->second.status = JobStatus::Assigned;
            it->second.assignedWorkerId = workerId;
        }
    }

    void completeJob(const std::string& jobId) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = jobs.find(jobId);
        if (it != jobs.end()) {
            it->second.status = JobStatus::Completed;
        }
    }

    void requeueJob(const std::string& jobId) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = jobs.find(jobId);
        if (it != jobs.end()) {
            it->second.status = JobStatus::Pending;
            it->second.assignedWorkerId = "";
        }
    }

    std::string getPayload(const std::string& jobId) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = jobs.find(jobId);
        return (it != jobs.end()) ? it->second.payload : "";
    }

    std::string findPendingJob() {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto& pair : jobs) {
            if (pair.second.status == JobStatus::Pending) return pair.first;
        }
        return "";
    }
};

#endif // JOB_H
