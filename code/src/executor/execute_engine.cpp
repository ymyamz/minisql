#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
#include <set>

//输出一列信息
void WriteOneColumn(string column_name, vector<string> result){
  std::stringstream ss;
  ResultWriter writer(ss);
  size_t data_width = column_name.size();
  //获取最长的长度
  for(auto it : result)
    data_width = max(data_width, it.size());
    
  //设置分割
  vector<int> V;
  V.resize(1);
  V[0] = data_width;
  writer.Divider(V);

  writer.BeginRow();
  writer.WriteHeaderCell(column_name, data_width);
  writer.EndRow();
  //读入vector内容
  for(auto it : result){
      writer.BeginRow();
      writer.WriteCell(it, data_width);
      writer.EndRow();
  }
  writer.EndInformation(result.size(), -1, false);//-1表示没有计算时间
  std::cout << writer.stream_.rdbuf();
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;

    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      //cout<<"plan create!";
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  //cout<<"execut start!";
  
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);//赋值之后context里就有相关信息
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}


void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

//下面的所有函数ast是语法树上的节点，然后context中是我们可以使用的工具catalog_manager之类的
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;

  string name(ast->child_->val_);//获取name
  if(this->dbs_.count(name)) return DB_ALREADY_EXIST;//如果该数据库名字已经存在
  try{
    this->dbs_[name] = new DBStorageEngine(name);
    current_db_ = name;
  }catch(logic_error &error){
    LOG(INFO) << error.what() << std::endl;
    return DB_FAILED;
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string name(ast->child_->val_);//获取name
  if(!this->dbs_.count(name)) return DB_NOT_EXIST;

  delete this->dbs_[name];//指针释放
  this->dbs_.erase(name);

  //删除对应的文件，参考打开代码
  char path[] = "./databases";
  DIR *dir = opendir(path);
  assert(dir != nullptr);
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    if((string)(stdir->d_name) == name){
      if(remove(((string)path + "/" + name).c_str()) == 0)
        LOG(INFO) << "Drop database" + name + "successfully" << endl;
      else
        throw "delete database failed";
    }
  }
  closedir(dir);

  if(current_db_ == name)//但前db如果被删除
    current_db_.resize(0);
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  vector<string> result(0);
  for(auto it: this->dbs_)
    result.emplace_back(it.first);
  WriteOneColumn("DATABASE", result);//输出一行
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string name(ast->child_->val_);//获取name
  if(!this->dbs_.count(name)) return DB_NOT_EXIST;
  current_db_ = name;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto manager = context->GetCatalog();
  vector<TableInfo*> table_infos;
  vector<string> out_list(0);
  dberr_t status = manager->GetTables(table_infos);
  if(status) return status;//不成功就返回错误信息
  for(auto it: table_infos)//成功则输出
    out_list.emplace_back(it->GetTableName());
  WriteOneColumn("TABLEs OF " + current_db_, out_list);
  return DB_SUCCESS;
}

//ast->child table_name ->next column合集->child是具体column
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto manager = context->GetCatalog();
  vector<Column*> col(0);
  string table_name(ast->child_->val_);//获取name
  auto ColNode = ast->child_->next_->child_;//段信息链表第一个
  int size = 0;//column个数
  LOG(INFO) << current_db_ << ' ' << table_name << endl;
  set<string> PK;//primary
  PK.clear();
  //获取primary keys
  for(auto it = ColNode; it != nullptr; it = it->next_){
      if(it->val_ != nullptr && strcmp(it->val_, "primary keys") == 0){
      for(auto y = it->child_; y != nullptr; y = y->next_)
        PK.insert(y->val_);
      break;//找到了pk
    }
  }
  //unique信息记录
  vector<string> unique_list(0);

  //遍历段信息
  for(;ColNode!=nullptr;ColNode = ColNode->next_){
    if(ColNode->val_ != nullptr && strcmp(ColNode->val_, "primary keys") == 0)//primary key就退出
      break;
    size++;
    col.emplace_back(nullptr);
    auto node = ColNode->child_;//name->type
    string colname(node->val_);
    string coltype(node->next_->val_);
    TypeId type = TypeId::kTypeInvalid;
    //type
    if(coltype == "int")type = TypeId::kTypeInt;
    else if(coltype == "float") type = TypeId::kTypeFloat;
    else if(coltype == "char") type = TypeId::kTypeChar;
    //unique
    bool unique =(ColNode->val_ != nullptr && strcmp(ColNode->val_, "unique") == 0);
    if(unique){
      unique_list.push_back(colname);
    }
    //nullable
    int nullable = true;
    if(PK.count(colname)) nullable = false;//如果是主建
    //创建
    if(type == TypeId::kTypeChar){
      int length = atoi(node->next_->child_->val_);//长度记录
      auto temp = new Column(colname, type, length, size, nullable, unique);
      col[size - 1] =temp;
    }
    else{
      auto temp = new Column(colname, type, size, nullable, unique);
      col[size - 1] =temp;
    }
  }

  Schema* schema = new Schema(col);
  TableInfo* info = nullptr;
  dberr_t error = manager->CreateTable(table_name, schema, nullptr, info);
  if(error) return error;

  //先创建主键上的index
  vector<string> keymap(0);
  for(auto it : PK)
    keymap.emplace_back(it);
  IndexInfo* index_info = nullptr;
  error = manager->CreateIndex(table_name, "PK OF " + table_name, keymap, nullptr, index_info, "bpTree");
  //cout << "CreateIndex" << endl;
  if(error) return error;
  
  //unique上的索引
  for(auto x : unique_list){//找到对应的属性
  cout << x << endl;
    index_info = nullptr;
    keymap.clear();
    keymap.emplace_back(x);
    error = manager->CreateIndex(table_name, "UNIQUE " + x + " OF " + table_name, keymap, nullptr, index_info, "bpTree");
    if(error) return error;
  }
  return DB_SUCCESS;

}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif  
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto manager = context->GetCatalog();
  string tablename(ast->child_->val_);//获取name
  dberr_t status = manager->DropTable(tablename);
  if(status)return status;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto manager = context->GetCatalog();
  vector<TableInfo*> table_infos;
  vector<IndexInfo*> index_infos;
  vector<string> out_list(0);
  dberr_t status = manager->GetTables(table_infos);
  if(status)return status;
  for(auto it : table_infos){//遍历当前
    status = manager->GetTableIndexes(it->GetTableName(), index_infos);
    if(status)return status;
    for(auto y : index_infos)
      out_list.emplace_back(it->GetTableName() + "." + y->GetIndexName());
  }
  WriteOneColumn("Indexes", out_list);
  return DB_SUCCESS;
}

//ast孩子节点存index name -> 存table_name -> 存索引列合集
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto manager = context->GetCatalog();

  auto index_node = ast->child_;
  string index_name(index_node->val_);
  string table_name(index_node->next_->val_);
  vector<string> KeyMap(0);

  //多个值生成kaymap
  for(auto col_node = index_node->next_->next_->child_; col_node != nullptr; col_node = col_node->next_)
    KeyMap.emplace_back(string(col_node->val_));

  IndexInfo* index_info;
  dberr_t status = manager->CreateIndex(table_name, index_name, KeyMap, nullptr, index_info, "b+Tree");
  return status;
}


dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto manager = context->GetCatalog();
  string index_name(ast->child_->val_);//获得要drop的index_name
  bool found = false;
  vector<TableInfo*> table_infos;
  IndexInfo* info = nullptr;

  dberr_t status = manager->GetTables(table_infos);
  if(status) return status;//若不成功，返回
  for(auto it : table_infos){
    status = manager->GetIndex(it->GetTableName(), index_name, info);
    if(status) continue;//如果没找到，进入下一个table_info中查找
    if(!status){//找到了
      found = true;
      dberr_t status = manager->DropIndex(it->GetTableName(), index_name);
      if(status) return status;
      break;
    } 
  }
  if(!found) return DB_INDEX_NOT_FOUND;//都没找到
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}


//main.cpp
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif

  ifstream ifs;
  ifs.open(ast->child_->val_,ios::in);
  if(!ifs.is_open())//无法打开文件
    return DB_FAILED;
  const int buf_size = 1024;
  char cmd[buf_size];
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  uint32_t syntax_tree_id = 0;
  //对于文件记时
  auto start_time = std::chrono::system_clock::now();
  

  while (1) {
    // read from file，参考InputCommand 
    int i = 0;
    char ch;
    memset(cmd, 0, sizeof(cmd));
    while ((ch = ifs.get()) != ';' && ch != EOF) {//从文件中读入
      cmd[i++] = ch;
    }
    if(ch == EOF) break;//退出
    cmd[i] = ch;  // ;
    ifs.get();      // remove enter    
    
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } 

    auto result = Execute(MinisqlGetParserRootNode());//这里要自己处理，否则会循环定义

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    // ExecuteInformation(result);
    if (result == DB_QUIT) {
      break;
    }
    else if(result != DB_SUCCESS){
      return result;
    }

  }
  
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  cout<<"total time is: "<<duration_time/1000<<"s"<<endl;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
