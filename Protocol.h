#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string>
#include <vector>
#include <sys/socket.h>

namespace MsgType {
    const std::string REGISTER = "REGISTER";
    const std::string PING = "PING";
    const std::string PONG = "PONG";
    const std::string JOB = "JOB";
    const std::string RESULT = "RESULT";
}

inline std::vector<std::string> splitMessage(const std::string& line) {
    std::vector<std::string> fields;
    size_t start = 0;
    size_t pos;

    while ((pos = line.find('|', start)) != std::string::npos) {
        fields.push_back(line.substr(start, pos - start));
        start = pos + 1;
    }
    fields.push_back(line.substr(start));

    return fields;
}

inline std::string readLine(int socket) {
    std::string line;
    char c;

    while (true) {
        ssize_t n = recv(socket, &c, 1, 0);
        if (n <= 0) {
            break;
        }
        if (c == '\n') {
            break;
        }
        line += c;
    }

    return line;
}

inline std::string buildJobMessage(const std::string& jobId, const std::string& payload) {
    return MsgType::JOB + "|" + jobId + "|" + payload + "\n";
}

inline std::string buildResultMessage(const std::string& jobId, const std::string& status) {
    return MsgType::RESULT + "|" + jobId + "|" + status + "\n";
}

inline std::string buildPing() {
    return MsgType::PING + "\n";
}

inline std::string buildPong() {
    return MsgType::PONG + "\n";
}

#endif // PROTOCOL_H
