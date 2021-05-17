//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  table_info_ = catalog->GetTable(table_oid);
}

void UpdateExecutor::Init() {
  assert(child_executor_ != nullptr);
  success_ = true;

  auto catalog = exec_ctx_->GetCatalog();

  auto txn = exec_ctx_->GetTransaction();
  auto table = table_info_->table_.get();
  const auto &schema = table_info_->schema_;
  RID rid;
  Tuple tuple(rid);

  while (child_executor_->Next(&tuple, &rid)) {
    Tuple new_tuple = GenerateUpdatedTuple(tuple);

    // update table
    bool success = table->UpdateTuple(new_tuple, rid, txn);
    if (!success) {
      success_ = false;
      break;
    }

    // update index
    auto indexes = catalog->GetTableIndexes(table_info_->name_);
    for (auto &index : indexes) {
      Tuple key = tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      Tuple new_key = new_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, rid, txn);
      index->index_->InsertEntry(new_key, rid, txn);
    }
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (success_) {
    success_ = false;
    return true;
  }

  return false;
}
}  // namespace bustub
