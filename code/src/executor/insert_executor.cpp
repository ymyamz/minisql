//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();//子结点初始化
}
bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  //cout<<"insert";
  if(!child_executor_->Next(row, rid)) return false;//没有下一个元组了直接退出
  TableInfo* table_info;//table_info信息读取
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
  //在table_heap上进行插入
  TableHeap* table_heap = table_info->GetTableHeap();
  string name = plan_->GetTableName();
  //主键约束
  //判断primary key是否被满足
  auto columns = table_info->GetSchema()->GetColumns();
  IndexInfo* indexinfo = nullptr;
  vector<RowId> result;
  result.clear();
  Row* keyrow = new Row(*rid);//initialize rid
  //判断primarykey是否被满足
  auto error=exec_ctx_->GetCatalog()->GetIndex(name, "PK OF " + name, indexinfo);
  if(error){
    throw "[INFO]insert primary check index fetch error!";
  }
  row->GetKeyFromRow(table_info->GetSchema(), indexinfo->GetIndexKeySchema(), *keyrow);
   auto d=indexinfo->GetIndex();
  indexinfo->GetIndex()->ScanKey(*keyrow, result, nullptr);
   
  if(!result.empty()){
    LOG(INFO) << "duplicate primary key" << endl;
    return false;
  }
  //判断unique是否被满足
  for(auto x : columns)
    if(x->IsUnique()){
      exec_ctx_->GetCatalog()->GetIndex(name, "UNIQUE " + x->GetName() + " OF " + name, indexinfo);
      row->GetKeyFromRow(table_info->GetSchema(), indexinfo->GetIndexKeySchema(), *keyrow);
      indexinfo->GetIndex()->ScanKey(*keyrow, result, nullptr);
      if(!result.empty()){
        LOG(INFO) << "duplicate unique field : " << x->GetName() << endl;
        return false;
      }
    }
  bool flag=table_heap->InsertTuple(*row, nullptr);//记录是否成功，获得row_id
  *rid = row->GetRowId();
  if(!flag)
    throw "InsertExecutor Insert Fail";
  // 更新index
  vector<IndexInfo*> indexs(0);
  exec_ctx_->GetCatalog()->GetTableIndexes(name, indexs);
  Row* key = new Row(*rid);//initialize rid
  for(auto it : indexs){
    
    row->GetKeyFromRow(table_info->GetSchema(), it->GetIndexKeySchema(), *key);//按照index_info获取key内容
    it->GetIndex()->InsertEntry(*key, *rid, nullptr);//new生成key
  }
  delete key;
  row = nullptr;//返回空的 tuple 
  return true;


}