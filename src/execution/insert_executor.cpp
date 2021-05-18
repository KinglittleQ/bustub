//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  success_ = true;
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  auto table_metadata = catalog->GetTable(table_oid);
  auto table = table_metadata->table_.get();
  const auto &schema = table_metadata->schema_;
  auto txn = exec_ctx_->GetTransaction();

  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    for (auto &value : values) {
      Tuple tuple(value, &schema);
      RID rid;

      // insert into table
      bool success = table->InsertTuple(tuple, &rid, txn);
      if (!success) {
        success_ = false;
        break;
      }

      // insert into index
      auto indexes = catalog->GetTableIndexes(table_metadata->name_);
      for (auto &index : indexes) {
        Tuple key = tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, rid, txn);
      }
    }
  } else {
    RID rid;
    Tuple tuple(rid);
    while (true) {
      bool has_more = child_executor_->Next(&tuple, &rid);
      if (!has_more) {
        break;
      }

      // insert into table
      bool success = table->InsertTuple(tuple, &rid, txn);
      if (!success) {
        success_ = false;
        break;
      }

      // insert into index
      auto indexes = catalog->GetTableIndexes(table_metadata->name_);
      for (auto &index : indexes) {
        Tuple key = tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, rid, txn);
      }
    }
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (success_) {
    success_ = false;
    return true;
  }

  return false;
}

}  // namespace bustub
