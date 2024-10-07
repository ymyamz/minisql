//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
    child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if(!child_executor_->Next(row, rid)) return false;//没有下一个元组了直接退出
  TableInfo* table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
  //在table_heap上进行检索
  TableHeap* table_heap = table_info->GetTableHeap();

  //获取新元组
  Row new_row = GenerateUpdatedTuple(*row);
  //修改表
  table_heap->UpdateTuple(new_row,*rid, nullptr);

  //先删除后插入index
  vector<IndexInfo*> indexs(0);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexs);

  Row* key = new Row(*rid);//initialize rid
  for(auto it : indexs){
    row->GetKeyFromRow(table_info->GetSchema(), it->GetIndexKeySchema(), *key);//按照index_info获取key内容
    it->GetIndex()->RemoveEntry(*key, *rid, nullptr);//删除
  }
  for(auto it : indexs){
    new_row.GetKeyFromRow(table_info->GetSchema(), it->GetIndexKeySchema(), *key);//按照index_info获取key内容
    it->GetIndex()->InsertEntry(*key, *rid, nullptr);//插入
  }
  delete key;
  row = nullptr;//返回空的row
  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {

  auto UpdatedAddr = plan_->GetUpdateAttr();
  Row row(src_row);
  vector<Field> new_fields;
  new_fields.clear();
  for(int i = 0; i < src_row.GetFieldCount(); i++)
    if(UpdatedAddr.count(i))
      new_fields.emplace_back(UpdatedAddr[i]->Evaluate(&row));//估计替换为ConstantValueExpression
    else
      new_fields.emplace_back(*row.GetField(i));//没有更新的属性
  return Row(new_fields);
}