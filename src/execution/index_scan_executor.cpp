//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), iterator_(nullptr, nullptr, 0) {
  auto catalog = exec_ctx->GetCatalog();
  auto index_oid = plan->GetIndexOid();
  auto index_info = catalog->GetIndex(index_oid);

  auto table_metadata = catalog->GetTable(index_info->table_name_);
  table_ = table_metadata->table_.get();

  auto index = index_info->index_.get();
  index_ = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index);

  schema_ = &(table_metadata->schema_);
  predicate_ = plan->GetPredicate();
}

void IndexScanExecutor::Init() { iterator_ = index_->GetBeginIterator(); }

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iterator_ != index_->GetEndIterator()) {
    auto id = (*iterator_).second;
    ++iterator_;

    bool success = table_->GetTuple(id, tuple, exec_ctx_->GetTransaction());
    assert(success);
    bool passed = predicate_->Evaluate(tuple, schema_).GetAs<bool>();
    if (passed) {
      *rid = id;
      return true;
    }
  }

  return false;
}

}  // namespace bustub
