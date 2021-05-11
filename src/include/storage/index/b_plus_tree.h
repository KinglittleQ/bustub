//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <deque>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

 private:
  bool StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N>
  N *Split(N *node);

  template <typename N>
  void CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  void Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  template <typename N = BPlusTreePage>
  N *GetNode(page_id_t page_id, int latch_type) {
    // the page may already be deleted, but it still can be read.
    // We won't write to it so it's safe here.
    auto page = buffer_pool_manager_->FetchPage(page_id);
    assert(page);
    if (latch_type == 1) {
      page->WLatch();
      latched_pages_.push_back(page);
      id_to_pages_[page_id] = page;
    } else if (latch_type == 2) {
      page->RLatch();
      latched_pages_.push_back(page);
      id_to_pages_[page_id] = page;
    }

    pinned_pages_.push_back(page_id);
    return reinterpret_cast<N *>(page->GetData());
  }

  template <typename N>
  N *NewNode(page_id_t *page_id, int latch_type) {
    auto page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
    }
    if (latch_type == 1) {
      page->WLatch();
      latched_pages_.push_back(page);
      id_to_pages_[*page_id] = page;
    } else if (latch_type == 2) {
      page->RLatch();
      latched_pages_.push_back(page);
      id_to_pages_[*page_id] = page;
    }

    pinned_pages_.push_back(*page_id);
    return reinterpret_cast<N *>(page->GetData());
  }

  void UnpinPages(bool is_write, size_t remained_size) {
    while (pinned_pages_.size() > remained_size) {
      auto page_id = pinned_pages_.front();
      pinned_pages_.pop_front();
      buffer_pool_manager_->UnpinPage(page_id, is_write);
    }
  }

  void UnlatchPages(bool is_write, size_t remained_size) {
    while (latched_pages_.size() > remained_size) {
      auto page = latched_pages_.front();
      latched_pages_.pop_front();
      id_to_pages_.erase(page->GetPageId());
      if (is_write) {
        page->WUnlatch();
      } else {
        page->RUnlatch();
      }
    }
  }

  void UnpinLastPage(bool is_write) {
    assert(!pinned_pages_.empty());
    auto page_id = pinned_pages_.back();
    pinned_pages_.pop_back();
    buffer_pool_manager_->UnpinPage(page_id, is_write);
  }

  void UnlatchLastPage(bool is_write) {
    assert(!latched_pages_.empty());
    auto page = latched_pages_.back();
    latched_pages_.pop_back();
    id_to_pages_.erase(page->GetPageId());
    if (is_write) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
  }

  void DeletePages() {
    while (!deleted_pages_.empty()) {
      auto page_id = deleted_pages_.front();
      deleted_pages_.pop_front();
      // buffer_pool_manager_->DeletePage(page_id);  // may fail here
      bool success;
      do {
        success = buffer_pool_manager_->DeletePage(page_id);
      } while (!success);
    }
  }

  inline static thread_local std::deque<page_id_t> pinned_pages_;
  inline static thread_local std::deque<Page *> latched_pages_;
  inline static thread_local std::unordered_map<page_id_t, Page *> id_to_pages_;
  inline static thread_local std::deque<page_id_t> deleted_pages_;

  // member variable
  std::string index_name_;
  std::atomic<page_id_t> root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
