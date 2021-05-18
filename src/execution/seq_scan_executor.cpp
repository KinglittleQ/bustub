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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iterator_(nullptr, RID(), nullptr) {
  auto catalog = exec_ctx->GetCatalog();
  auto table_oid = plan->GetTableOid();
  auto table = catalog->GetTable(table_oid);

  table_heap_ = table->table_.get();
  schema_ = &table->schema_;
  predicate_ = plan->GetPredicate();

  const auto output_schema = plan_->OutputSchema();
  for (uint32_t i = 0; i < output_schema->GetColumnCount(); i++) {
    const auto &col = output_schema->GetColumn(i);
    uint32_t idx = schema_->GetColIdx(col.GetName());
    attrs_.push_back(idx);
  }
}

void SeqScanExecutor::Init() { iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto &it = iterator_;

  while (it != table_heap_->End()) {
    bool passed = true;

    if (predicate_ != nullptr) {
      passed = predicate_->Evaluate(&(*it), schema_).GetAs<bool>();
    }
    if (passed) {
      *rid = it->GetRid();
      *tuple = it->KeyFromTuple(*schema_, *GetOutputSchema(), attrs_);
      it++;
      return true;
    }
    it++;
  }

  return false;
}

}  // namespace bustub
