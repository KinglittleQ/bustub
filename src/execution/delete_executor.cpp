//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  table_info_ = catalog->GetTable(table_oid);
}

void DeleteExecutor::Init() {
  assert(child_executor_ != nullptr);
  child_executor_->Init();

  success_ = true;

  auto catalog = exec_ctx_->GetCatalog();

  auto txn = exec_ctx_->GetTransaction();
  auto table = table_info_->table_.get();

  RID rid;
  Tuple tuple(rid);

  auto indexes = catalog->GetTableIndexes(table_info_->name_);

  // get key attrs
  const auto &schema = table_info_->schema_;
  const auto &child_schema = child_executor_->GetOutputSchema();
  std::vector<std::vector<uint32_t>> attrs(indexes.size());
  int index_idx = 0;
  for (auto &index : indexes) {
    for (uint32_t i = 0; i < index->key_schema_.GetColumnCount(); i++) {
      uint32_t col_idx = index->index_->GetKeyAttrs()[i];
      const auto &col = schema.GetColumn(col_idx);
      uint32_t idx = child_schema->GetColIdx(col.GetName());
      attrs[index_idx].push_back(idx);
    }
    index_idx++;
  }

  while (child_executor_->Next(&tuple, &rid)) {
    // delete from table
    bool success = table->MarkDelete(rid, txn);
    if (!success) {
      success_ = false;
      break;
    }

    // delete from index
    int k = 0;
    for (auto &index : indexes) {
      Tuple key = tuple.KeyFromTuple(schema, index->key_schema_, attrs[k]);
      index->index_->DeleteEntry(key, rid, txn);
      k++;
    }
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (success_) {
    success_ = false;
    return true;
  }

  return false;
}

}  // namespace bustub
