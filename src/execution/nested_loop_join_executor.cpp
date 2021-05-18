//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  // left
  left_schema_ = plan->GetLeftPlan()->OutputSchema();

  // right
  right_schema_ = plan->GetRightPlan()->OutputSchema();

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

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  hash_left_tuple_ = false;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID left_rid;
  if (!hash_left_tuple_) {
    bool has_more = left_executor_->Next(&left_tuple_, &left_rid);
    if (!has_more) {
      return false;
    }
  }

  auto &left_tuple = left_tuple_;
  Tuple right_tuple;
  RID right_rid;
  bool has_more = right_executor_->Next(&right_tuple, &right_rid);
  if (!has_more) {
    hash_left_tuple_ = false;
    return Next(tuple, rid);
  }

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
