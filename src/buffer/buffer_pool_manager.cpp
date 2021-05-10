//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManager::FindOneFreePage(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    // find a free page in freelist and pop it
    *frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // evict one page
    if (!replacer_->Victim(frame_id)) {
      return false;
    }
    auto &page = pages_[*frame_id];

    // delete it from the page table
    page_table_.erase(page_table_.find(page.page_id_));

    // write it to the disk if dirty
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.page_id_, page.GetData());
    }
  }

  return true;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::lock_guard<std::mutex> lock_guard(latch_);

  frame_id_t frame_id;

  if (page_table_.find(page_id) != page_table_.end()) {
    // the page already exists
    frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_ += 1;
  } else {
    // find one free page
    if (!FindOneFreePage(&frame_id)) {
      return nullptr;
    }

    // load the new page
    auto &page = pages_[frame_id];
    page.page_id_ = page_id;
    page.pin_count_ = 1;
    page.is_dirty_ = false;
    disk_manager_->ReadPage(page_id, page.GetData());

    // insert into the page table
    page_table_[page_id] = frame_id;
  }

  return pages_ + frame_id;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty_) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    // page_id is invalid
    return true;
  }

  auto frame_id = page_table_[page_id];
  auto &page = pages_[page_table_[page_id]];

  assert(page.pin_count_ > 0);
  page.pin_count_ -= 1;

  page.is_dirty_ |= is_dirty_;

  if (page.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    // page_id is invalid
    return false;
  }

  auto &page = pages_[page_table_[page_id]];
  disk_manager_->WritePage(page.page_id_, page.GetData());
  page.is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::lock_guard<std::mutex> lock_guard(latch_);

  frame_id_t frame_id;
  if (!FindOneFreePage(&frame_id)) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  auto &page = pages_[frame_id];
  *page_id = disk_manager_->AllocatePage();

  // set meta data
  page.page_id_ = *page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;
  page.ResetMemory();

  // insert into the page table
  page_table_[*page_id] = frame_id;

  return pages_ + frame_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::lock_guard<std::mutex> lock_guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    // page_id does not exist in the buffer pool
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  auto frame_id = page_table_[page_id];
  auto &page = pages_[frame_id];

  if (page.pin_count_ != 0) {
    return false;
  }

  disk_manager_->DeallocatePage(page_id);

  // now the page is still in the replacer, we should remove it
  // and add it into the freelist
  replacer_->Pin(frame_id);
  free_list_.push_back(frame_id);

  page.is_dirty_ = false;
  page.pin_count_ = 0;
  page.page_id_ = INVALID_PAGE_ID;

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> lock_guard(latch_);

  for (const auto &pair : page_table_) {
    auto &page = pages_[pair.second];
    disk_manager_->WritePage(page.page_id_, page.GetData());
    page.is_dirty_ = false;
  }
}

}  // namespace bustub
