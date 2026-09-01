#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <string>
#include "RingBuffer.h"

void basicTest() {
    std::cout << "\n--- basic push/pop ---\n";
    RingBuffer<int> rb(4);

    for (int i = 0; i < 4; i++) {
        bool ok = rb.tryPush(i);
        std::cout << "push " << i << ": " << (ok ? "ok" : "FAILED") << "\n";
    }

    bool overfull = rb.tryPush(99);
    std::cout << "push while full: " << (overfull ? "unexpectedly succeeded!" : "correctly failed") << "\n";

    int val;
    for (int i = 0; i < 4; i++) {
        bool ok = rb.tryPop(val);
        std::cout << "pop: " << (ok ? std::to_string(val) : "FAILED") << " (expected " << i << ")\n";
    }

    bool emptyPop = rb.tryPop(val);
    std::cout << "pop while empty: " << (emptyPop ? "unexpectedly succeeded!" : "correctly failed") << "\n";
}

void wraparoundTest() {
    std::cout << "\n--- wraparound (multiple laps) ---\n";
    RingBuffer<int> rb(4);
    for (int i = 0; i < 10; i++) {
        rb.tryPush(i);
        int v;
        rb.tryPop(v);
        std::cout << "lap test " << i << ": got " << v
                   << (v == i ? " OK" : " MISMATCH") << "\n";
    }
}

void stressTest() {
    std::cout << "\n--- concurrent stress test ---\n";
    RingBuffer<int> rb(1024);
    const int numProducers = 4, numConsumers = 4, itemsPerProducer = 10000;
    std::atomic<int> totalPushed{0}, totalPopped{0};
    std::atomic<long long> sumPushed{0}, sumPopped{0};
    std::atomic<bool> producersDone{false};
    std::vector<std::thread> producers, consumers;

    for (int p = 0; p < numProducers; p++) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < itemsPerProducer; i++) {
                int value = p * itemsPerProducer + i;
                while (!rb.tryPush(value)) { }
                totalPushed++;
                sumPushed += value;
            }
        });
    }

    for (int c = 0; c < numConsumers; c++) {
        consumers.emplace_back([&]() {
            while (!producersDone.load() || totalPopped.load() < totalPushed.load()) {
                int value;
                if (rb.tryPop(value)) {
                    totalPopped++;
                    sumPopped += value;
                }
            }
        });
    }

    for (auto& t : producers) t.join();
    producersDone = true;
    for (auto& t : consumers) t.join();

    std::cout << "pushed=" << totalPushed << " popped=" << totalPopped << "\n";
    std::cout << "sumPushed=" << sumPushed << " sumPopped=" << sumPopped << "\n";
    std::cout << ((totalPushed == totalPopped && sumPushed == sumPopped) ? "PASS" : "FAIL") << "\n";
}

int main() {
    basicTest();
    wraparoundTest();
    stressTest();
    return 0;
}
