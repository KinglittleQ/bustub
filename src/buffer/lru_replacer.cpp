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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  refs_.resize(num_pages);
  size_ = 0;
  arm_ = 0;
  std::fill(refs_.begin(), refs_.end(), INVALID_REF_BIT);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (size_ == 0) {
    return false;
  }

  bool found = false;
  while (!found) {
    if (refs_[arm_] == 1) {
      refs_[arm_] = 0;
    } else if (refs_[arm_] == 0) {
      *frame_id = arm_;
      refs_[arm_] = INVALID_REF_BIT;
      size_--;
      found = true;
    }

    arm_ = (arm_ + 1) % refs_.size();
  }

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (refs_[frame_id] != INVALID_REF_BIT) {
    refs_[frame_id] = INVALID_REF_BIT;
    size_--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (refs_[frame_id] == INVALID_REF_BIT) {
    refs_[frame_id] = 1;
    size_++;
  }
}

size_t LRUReplacer::Size() { return size_; }

}  // namespace bustub
