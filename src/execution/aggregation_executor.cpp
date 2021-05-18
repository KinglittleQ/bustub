//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  RID rid;
  Tuple tuple(rid);

  while (child_->Next(&tuple, &rid)) {
    auto key = MakeKey(&tuple);
    auto value = MakeVal(&tuple);
    aht_.InsertCombine(key, value);
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  auto &it = aht_iterator_;
  const auto output_schema = GetOutputSchema();

  for (; it != aht_.End(); ++it) {
    auto key = it.Key();
    auto val = it.Val();

    std::vector<Value> values;
    for (uint32_t i = 0; i < output_schema->GetColumnCount(); i++) {
      const auto &col = output_schema->GetColumn(i);
      const auto expr = col.GetExpr();
      values.push_back(expr->EvaluateAggregate(key.group_bys_, val.aggregates_));
    }

    Tuple new_tuple(values, output_schema);
    auto having = plan_->GetHaving();
    bool passed = true;
    if (having != nullptr) {
      passed = having->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>();
    }
    if (passed) {
      *tuple = new_tuple;
      ++it;
      return true;
    }
  }

  return false;
}

}  // namespace bustub
