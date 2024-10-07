//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),now(0){Answer.clear();}

void SeqScanExecutor::Init() {
  //printf("seq_scan start\n");
  //答案记录初始化
  Answer.clear();

  TableInfo* table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
  //在table_heap上进行检索
  TableHeap* table_heap = table_info->GetTableHeap();
  auto it = table_heap->Begin(nullptr);//指针 begin
  auto right = Field(kTypeInt, 1);//相当于bool true
  while(it != table_heap->End()){
    Row row = *it;//指针指向的row
    // LOG(INFO) << (*it->GetField(0)).toString() << endl;
    //从下往上调用
    if(plan_->GetPredicate() == NULL || plan_->filter_predicate_->Evaluate(&row).CompareEquals(right) == kTrue){
      Answer.emplace_back(row);
    }
    it++;
  }
  now = 0;
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  if(now == Answer.size()) return false;//如果到底了
  *row = Answer[now++];
  *rid = row->GetRowId();//下一个
  return true;
}
