//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"


DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}
//和insert实现类似
bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {

  if(!child_executor_->Next(row, rid)) return false;//没有下一个元组了直接退出
  TableInfo* table_info;//table_info信息读取
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
  //在table_heap上进行删除
  TableHeap* table_heap = table_info->GetTableHeap();
  bool flag=table_heap->MarkDelete(*rid, nullptr);//记录是否成功，获得row_id
  
  if(!flag)
    throw "InsertExecutor Insert Fail";
  // 更新index
  vector<IndexInfo*> indexs(0);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexs);

  Row* key = new Row(*rid);//initialize rid
  for(auto it : indexs){
    row->GetKeyFromRow(table_info->GetSchema(), it->GetIndexKeySchema(), *key);//按照index_info获取key内容
    it->GetIndex()->RemoveEntry(*key, *rid, nullptr);//new生成key
  }
  delete key;
  row = nullptr;//返回空的 tuple 
  return true;
}