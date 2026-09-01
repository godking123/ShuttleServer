#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <vector>
#include <atomic>
#include <iostream>
#include <memory>

template<typename T>
class RingBuffer {
public:
    explicit RingBuffer(size_t capacity);

    bool tryPush(T item);
    bool tryPop(T& item);

private:
    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    std::unique_ptr<Cell[]> buffer;
    size_t capacityMask;
    alignas(64) std::atomic<size_t> enqueuePos{0};
    alignas(64) std::atomic<size_t> dequeuePos{0};
};

template<typename T>
RingBuffer<T>::RingBuffer(size_t capacity) : capacityMask(capacity - 1) {
    buffer = std::make_unique<Cell[]>(capacity);
    for (size_t i = 0; i < capacity; ++i) {
        buffer[i].sequence = i;
    }
};

template<typename T>
bool RingBuffer<T>::tryPush(T item) {
    size_t pos = enqueuePos.load();
    while (true) {
        size_t index = pos & capacityMask;
        long diff = (long)buffer[index].sequence.load() - (long)pos;

        if (diff == 0) {
            if (enqueuePos.compare_exchange_weak(pos, pos + 1)) {
                buffer[index].data = std::move(item);
                buffer[index].sequence.store(pos + 1);
                return true;
            }
            // CAS failed — pos was updated automatically, loop continues
        } else if (diff < 0) {
            return false;  // buffer full
        } else {
            pos = enqueuePos.load();  // another producer got ahead — resync
        }
    }
}

template<typename T>
bool RingBuffer<T>::tryPop(T& item) {
    size_t pos = dequeuePos.load();
    while (true) {
        size_t index = pos & capacityMask;
        long diff = (long)buffer[index].sequence.load() - (long)(pos + 1);

        if (diff == 0) {
            // ready to pop
            if (dequeuePos.compare_exchange_weak(pos, pos + 1)) {
                item = std::move(buffer[index].data);
                buffer[index].sequence.store(pos + capacityMask + 1); // = pos + capacity
                return true;
            }
            // CAS failed — pos auto-updated, loop and recheck
        } else if (diff < 0) {
            return false;  // nothing here yet — buffer's empty at this slot
        } else {
            pos = dequeuePos.load();  // another consumer got ahead — resync
        }
    }
}

#endif //RING_BUFFER_H_
