//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetLSN();  // invalid lsn
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int idx = 0; idx < GetSize(); idx++) {
    if (array[idx].second == value) {
      return idx;
    }
  }

  assert(false);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // int i;
  // for (i = 1; i < GetSize(); i++) {
  //   if (comparator(key, KeyAt(i)) < 0) {
  //     break;
  //   }
  // }
  // return ValueAt(i - 1);

  // binary search
  // assert(GetSize() > 1);
  int left = 1;
  int right = GetSize() - 1;
  int result;

  while (true) {
    if (left > right) {
      result = right;
      break;
    }
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  return ValueAt(result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  assert(GetSize() == 0);  // empty node
  assert(IsRootPage());

  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;

  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int idx = ValueIndex(old_value);
  memmove(reinterpret_cast<char *>(array + idx + 2), reinterpret_cast<char *>(array + idx + 1),
          sizeof(MappingType) * (GetSize() - idx - 1));
  array[idx + 1].first = new_key;
  array[idx + 1].second = new_value;
  IncreaseSize(1);

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetSize() == 0);
  assert(GetSize() == GetMaxSize());

  int left_size = GetSize() / 2;
  int right_size = GetSize() - left_size;

  recipient->CopyNFrom(array + left_size, right_size, buffer_pool_manager);
  SetSize(left_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  memcpy(reinterpret_cast<char *>(array + GetSize()), reinterpret_cast<char *>(items), size * sizeof(MappingType));
  IncreaseSize(size);

  for (int i = 0; i < size; i++) {
    page_id_t page_id = items[i].second;
    UpdateParentIdOfPage(page_id, GetPageId(), buffer_pool_manager);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  memmove(reinterpret_cast<char *>(array + index), reinterpret_cast<char *>(array + index + 1),
          sizeof(MappingType) * (GetSize() - index - 1));
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  assert(GetSize() == 1);
  IncreaseSize(-1);
  return array[0].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;
  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetSize() < GetMinSize());
  assert(GetSize() > GetMinSize());

  recipient->array[recipient->GetSize()].second = array[0].second;
  recipient->array[recipient->GetSize()].first = middle_key;
  recipient->IncreaseSize(1);

  memmove(reinterpret_cast<char *>(array), reinterpret_cast<char *>(array + 1), sizeof(MappingType) * (GetSize() - 1));
  IncreaseSize(-1);

  // update parent id
  auto page_id = recipient->ValueAt(recipient->GetSize() - 1);
  UpdateParentIdOfPage(page_id, recipient->GetPageId(), buffer_pool_manager);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMinSize());

  array[GetSize()] = pair;
  IncreaseSize(1);

  // update parent id
  auto page_id = ValueAt(GetSize() - 1);
  UpdateParentIdOfPage(page_id, GetPageId(), buffer_pool_manager);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetSize() < GetMinSize());
  assert(GetSize() > GetMinSize());

  memmove(reinterpret_cast<char *>(recipient->array + 1), reinterpret_cast<char *>(recipient->array),
          sizeof(MappingType) * recipient->GetSize());
  recipient->array[0] = array[GetSize() - 1];
  recipient->array[1].first = middle_key;
  recipient->IncreaseSize(1);

  IncreaseSize(-1);

  // update parent id
  auto page_id = recipient->array[0].second;
  UpdateParentIdOfPage(page_id, GetPageId(), buffer_pool_manager);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  memmove(reinterpret_cast<char *>(array + 1), reinterpret_cast<char *>(array), sizeof(MappingType) * GetSize());
  array[0] = pair;
  IncreaseSize(1);

  auto page_id = pair.second;
  UpdateParentIdOfPage(page_id, GetPageId(), buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpdateParentIdOfPage(page_id_t page_id, page_id_t parent_page_id,
                                                          BufferPoolManager *bpm) {
  auto page = bpm->FetchPage(page_id);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(parent_page_id);
  bpm->UnpinPage(page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftArray(int offset) {
  if (offset > 0) {
    // right shift
    memmove(reinterpret_cast<char *>(array + offset), reinterpret_cast<char *>(array), sizeof(MappingType) * GetSize());
  } else if (offset < 0) {
    // left shift
    memmove(reinterpret_cast<char *>(array), reinterpret_cast<char *>(array - offset),
            sizeof(MappingType) * (GetSize() + offset));
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
