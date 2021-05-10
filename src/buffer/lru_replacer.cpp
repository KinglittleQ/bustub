//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (queue.empty()) {
    return false;
  }

  *frame_id = queue.back();
  queue.pop_back();
  pos.erase(*frame_id);

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (pos.count(frame_id) == 0) {
    return;
  }

  auto it = pos[frame_id];
  queue.erase(it);
  pos.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (pos.count(frame_id) == 0) {
    queue.push_front(frame_id);
    pos[frame_id] = queue.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock_guard(latch_);
  return queue.size();
}

}  // namespace bustub
