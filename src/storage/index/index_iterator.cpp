/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (node_) {
    bpm_->UnpinPage(node_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  if (node_ == nullptr and offset_ == 0) {
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(node_);
  assert(offset_ < node_->GetSize());
  return node_->GetItem(offset_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  offset_ += 1;
  if (offset_ == node_->GetSize()) {
    auto next_page_id = node_->GetNextPageId();
    bpm_->UnpinPage(node_->GetPageId(), false);
    if (next_page_id == INVALID_PAGE_ID) {
      node_ = nullptr;
      offset_ = 0;
    } else {
      auto page = bpm_->FetchPage(next_page_id);
      node_ = reinterpret_cast<LeafPage *>(page->GetData());
      offset_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
