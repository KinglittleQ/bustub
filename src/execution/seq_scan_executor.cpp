//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  auto catalog = exec_ctx->GetCatalog();
  auto table_oid = plan->GetTableOid();
  auto table = catalog->GetTable(table_oid);

  table_heap_ = table->table_.get();
  schema_ = &table->schema_;
  predicate_ = plan->GetPredicate();
}

void SeqScanExecutor::Init() {
  auto it = table_heap_->Begin(exec_ctx_->GetTransaction());
  iterator_= std::make_unique<TableIterator>(it);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto &it = *iterator_;

  while (it != table_heap_->End()) {
    bool passed = predicate_->Evaluate(&(*it), schema_).GetAs<bool>();
    if (passed) {
      *tuple = *it;
      *rid = tuple->GetRid();
      it++;
      return true;
    }
    it++;
  }

  return false;
}

}  // namespace bustub
