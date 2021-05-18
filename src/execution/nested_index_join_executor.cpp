//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto catalog = exec_ctx->GetCatalog();
  auto right_table_oid = plan->GetInnerTableOid();
  auto right_table_name = catalog->GetTable(right_table_oid)->name_;
  auto index_info = catalog->GetIndex(plan->GetIndexName(), right_table_name);

  index_ = index_info->index_.get();
  key_schema_ = index_->GetKeySchema();
  left_schema_ = plan->OuterTableSchema();
  right_schema_ = plan->InnerTableSchema();

  auto right_table_info = catalog->GetTable(right_table_oid);
  right_table_ = right_table_info->table_.get();

  // key attrs
  for (uint32_t i = 0; i < key_schema_->GetColumnCount(); i++) {
    const auto &col = key_schema_->GetColumn(i);
    uint32_t idx = left_schema_->GetColIdx(col.GetName());
    key_attrs_.push_back(idx);
  }

  // output schema
  const auto output_schema = plan_->OutputSchema();
  attrs_.resize(output_schema->GetColumnCount());
  for (uint32_t i = 0; i < output_schema->GetColumnCount(); i++) {
    auto col_name = output_schema->GetColumn(i).GetName();

    bool found = false;
    for (uint32_t j = 0; j < left_schema_->GetColumnCount() && !found; j++) {
      if (left_schema_->GetColumn(j).GetName() == col_name) {
        attrs_[i] = (j << 1);
        found = true;
      }
    }

    for (uint32_t j = 0; j < right_schema_->GetColumnCount() && !found; j++) {
      if (right_schema_->GetColumn(j).GetName() == col_name) {
        attrs_[i] = (j << 1) + 1;
        found = true;
      }
    }

    assert(found);
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID left_rid;
  RID right_rid;
  Tuple left_tuple(left_rid);
  Tuple right_tuple(right_rid);

  bool has_more = child_executor_->Next(&left_tuple, &left_rid);
  if (!has_more) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();

  auto key = left_tuple.KeyFromTuple(*left_schema_, *key_schema_, key_attrs_);
  std::vector<RID> result;
  index_->ScanKey(key, &result, txn);
  if (result.empty()) {
    return Next(tuple, rid);
  }
  assert(result.size() == 1);
  right_rid = result[0];

  bool success = right_table_->GetTuple(right_rid, &right_tuple, txn);
  assert(success);

  bool passed = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema_, &right_tuple, right_schema_).GetAs<bool>();
  if (!passed) {
    return Next(tuple, rid);
  }

  // combine two tuples
  std::vector<Value> values;
  for (uint32_t attr : attrs_) {
    bool is_left = (attr % 2 == 0);
    uint32_t idx = (attr >> 1);

    if (is_left) {
      values.push_back(left_tuple.GetValue(left_schema_, idx));
    } else {
      values.push_back(right_tuple.GetValue(right_schema_, idx));
    }
  }

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

}  // namespace bustub
