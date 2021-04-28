//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(LeafPage *node, BufferPoolManager *bpm, int offset = 0) : node_(node), bpm_(bpm), offset_(offset) {}
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { return node_ == itr.node_ and offset_ == itr.offset_; }

  bool operator!=(const IndexIterator &itr) const { return node_ != itr.node_ or offset_ != itr.offset_; }

 private:
  LeafPage *node_;
  BufferPoolManager *bpm_;
  int offset_;
  // add your own private member variables here
};

}  // namespace bustub
