#include "executor/executors/index_scan_executor.h"
#include <algorithm>
#include "planner/expressions/constant_value_expression.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), now(0) {
      answer.clear(), Answer.clear();
    }

bool cmp(const RowId& A, const RowId& B){
  return A.Get() < B.Get();
}

//具体结构可以见abstract_statement.h，这里我们需要遍历语法树来找到合法的点
void dfs(vector<ComparisonExpression*>& vec, AbstractExpressionRef now){//要递归地寻找，因为存在and的情况
  if(now.get()->GetType() == ExpressionType::LogicExpression){//只有and的情况
    dfs(vec, now->GetChildAt(0));//就只有左儿子和右儿子
    dfs(vec, now->GetChildAt(1));//就只有左儿子和右儿子
  }
  else if(now.get()->GetType() == ExpressionType::ComparisonExpression){//比较类型压入即可
    vec.emplace_back(dynamic_cast<ComparisonExpression*>(now.get()));
    return;
  }
  else{//这里不应该产生这样的树的结构
    throw "Index_Scan_Executor Error: 有其他类型的点";
  }
}

void IndexScanExecutor::Init() {//直接计算出所有符合index的答案，在Next中再进行挑选
  //cout<<"indexscan"<<endl;
  answer.clear(), Answer.clear();//初始化
  auto indexes = plan_->indexes_;//可用的index
  string name = plan_->GetTableName();
  auto predicate = plan_->GetPredicate();//找到语法上的根节点指针
  auto manager = exec_ctx_->GetCatalog();
  TableInfo* info = nullptr;
  manager->GetTable(name, info);
  vector<ComparisonExpression*> predicates(0);
  dfs(predicates, predicate);
  //use index
  //cout<<"size "<<indexes.size()<<endl;

  for(auto where : predicates)
    for (auto index : indexes) {
cout<<index->GetIndexName()<<endl;
      // if (index->GetIndexKeySchema()->GetColumns().size() == 1) {//单列索引，这里默认满足
      auto col_id = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() -1;
      auto col_expr = dynamic_cast<ColumnValueExpression*>(where->GetChildAt(0).get());//左儿子是col
      vector<Field> fields;
      fields.clear();
      fields.emplace_back(dynamic_cast<ConstantValueExpression *>(where->GetChildAt(1).get())->val_);

      Row key_row(fields);
      vector<RowId> temp1(answer);
      vector<RowId> temp2(0);
      
      if(col_expr->GetColIdx() == col_id){
          
        index->GetIndex()->ScanKey(key_row, temp2, nullptr, where->GetComparisonType());//scan key的参数还有问题？
        sort(temp1.begin(), temp1.end(), cmp);
        sort(temp2.begin(), temp2.end(), cmp);

        if(answer.empty())
          answer.swap(temp2);
        else
          std::set_intersection(temp1.begin(), temp1.end(), temp2.begin(), temp2.end(), insert_iterator<vector<RowId>>(answer,answer.begin()), cmp);//传入比较函数
      }
    }
  // cout << answer.size() << endl;
  //取出所有的row
  auto storage = info->GetTableHeap();
  vector<RowId> temp(0);
  auto right = Field(kTypeInt, 1);
  for(int i = 0; i < answer.size(); i++){
    auto x = answer[i];
    auto row = Row(x);
    // cout << x.Get() << ' ' << x.GetPageId() << endl; 
    storage->GetTuple(&row, nullptr);
    //最后用所有谓词扫描一遍
    if(!plan_->need_filter_ || plan_->filter_predicate_->Evaluate(&row).CompareEquals(right) == kTrue){
      Answer.emplace_back(row);
      temp.emplace_back(answer[i]);
    }
  }
  answer.swap(temp);//把temp给answer
  now = 0;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if(now == answer.size()) return false;
  *row = Answer[now];
  *rid = answer[now];
  now++;
  return true;
  //return false;
}
