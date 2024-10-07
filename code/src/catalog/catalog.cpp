#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return 12+8*table_meta_pages_.size()+8*index_meta_pages_.size();
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  std::atomic_init(&next_table_id_,0);
  std::atomic_init(&next_index_id_,0);
  //如果初次创建
  if(init){
      this->catalog_meta_ = CatalogMeta::NewInstance();
      this->next_index_id_ = 0;
      this->next_table_id_ = 0;
      this->table_names_.clear();
      this->tables_.clear();
      this->index_names_.clear();
      this->indexes_.clear();
      FlushCatalogMetaPage();//写入缓存
    return;
  }
  //如果不是初次，已经存在bufferpoolmanager中
  Page *meta_page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(meta_page != nullptr);
  catalog_meta_=CatalogMeta::DeserializeFrom(meta_page->GetData());
  buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  //构建表
  for(auto map : catalog_meta_->table_meta_pages_){
    page_id_t page_id=map.second;
    Page* table_meta_page=buffer_pool_manager_->FetchPage(page_id);
    TableMetadata *table_meta;
    TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
    //创建table_heap
    TableHeap *table_heap=TableHeap::Create(buffer_pool_manager,table_meta->GetFirstPageId(),
                                          table_meta->GetSchema(),log_manager,lock_manager);
    
    //需要创建table_info
    TableInfo* table_info=TableInfo::Create();
    table_info->Init(table_meta,table_heap);
    //更新catalog中table map
    table_names_[table_meta->GetTableName()]=table_meta->GetTableId();
    tables_[table_meta->GetTableId()]=table_info;

    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  //创建索引
  for(auto map: catalog_meta_->index_meta_pages_){
    page_id_t page_id=map.second;
    Page* index_meta_page=buffer_pool_manager_->FetchPage(page_id);
    IndexMetadata *index_meta;
    IndexMetadata::DeserializeFrom(index_meta_page->GetData(),index_meta);
    //创建index_info
    IndexInfo* index_info=IndexInfo::Create();
    index_info->Init(index_meta, tables_[index_meta->GetTableId()],buffer_pool_manager);
    //更新catalog中index_map
    auto table_name=tables_[index_meta->GetTableId()]->GetTableName();
    // map for indexes: table_name->index_name->index_id
    index_names_[table_name][index_meta->GetIndexName()]=index_meta->GetIndexId();
    indexes_[index_meta->GetIndexId()] = index_info;

    buffer_pool_manager_->UnpinPage(page_id, false);

  }


}

CatalogManager::~CatalogManager() {
  ASSERT(FlushCatalogMetaPage() == DB_SUCCESS, "CatalogManager flush meta page failed.");
  for (auto &it : catalog_meta_->table_meta_pages_) {
    buffer_pool_manager_->FlushPage(it.second);
  }
  for (auto &it : catalog_meta_->index_meta_pages_) {
    buffer_pool_manager_->FlushPage(it.second);
  }

}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  //如果已经存在
  if(table_names_.find(table_name)!=table_names_.end())
    return DB_TABLE_ALREADY_EXIST;
  //获得table_id
  auto table_id=catalog_meta_->GetNextTableId();//最后一个table_id+1
  //生成table_heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);//还没有first_page
  //生成table_meta
  TableMetadata* table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema);
  //获取table_meta_page
  page_id_t new_page_id;
  Page* new_page=buffer_pool_manager_->NewPage(new_page_id);
  table_meta->SerializeTo(new_page->GetData());
  buffer_pool_manager_->UnpinPage(new_page_id,true);
  //生成table_info
  table_info=TableInfo::Create();
  table_info->Init(table_meta,table_heap);
  //更新catalog table
  table_names_[table_name]=table_id;
  tables_[table_id]=table_info;
  catalog_meta_->table_meta_pages_[table_id]=new_page_id;//别忘记更新catalog_meta
  return DB_SUCCESS;
}


dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name)==table_names_.end())
    return DB_TABLE_NOT_EXIST;
  auto table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}


dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.empty()) return DB_FAILED;//如果空
  else{
    for (auto &info:tables_) {
      tables.push_back(info.second);
    }
    return DB_SUCCESS;
  }
  
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  
  
  
  
  
  //检查table
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;                                 
  //已经存在
  if ((index_names_[table_name].find(index_name) != index_names_[table_name].end())) return DB_INDEX_ALREADY_EXIST;
  //获得index_id
  auto index_id = catalog_meta_->GetNextIndexId();

  TableInfo* table_info=tables_[table_names_[table_name]];
  //生成key_map
  auto schema = table_info->GetSchema();
  std::vector<uint32_t> key_map;
  for (auto &it : index_keys) {
    uint32_t col_idx;
    if (schema->GetColumnIndex(it, col_idx) == DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(col_idx);
  }
  //生成index_meta
  auto index_meta = IndexMetadata::Create(index_id, index_name, table_names_[table_name], key_map);
  //生成index_meta_page
  page_id_t new_page_id;
  Page* new_page=buffer_pool_manager_->NewPage(new_page_id);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  index_meta->SerializeTo(new_page->GetData());
  //生成index_info
  index_info = IndexInfo::Create();
  index_info->Init(index_meta,table_info, buffer_pool_manager_);

  //更新catalog index
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;
  catalog_meta_->index_meta_pages_[index_id] = new_page_id;

  return DB_SUCCESS;
}


dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  //检查table
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  //检查index
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;

  auto index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);


  return DB_SUCCESS;
}


dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  //存在
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;

  for (auto &it : indexes_) {
    if (it.second->GetTableInfo()->GetTableName() == table_name) {//要求是同一个表的index
      indexes.emplace_back(it.second);
    }
  }
  return DB_SUCCESS;

}


dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;

  auto table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_.erase(table_id);
  //注意catalog_meta
  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  catalog_meta_->table_meta_pages_.erase(table_id);
  buffer_pool_manager_->DeletePage(page_id);
  return DB_SUCCESS;
}


dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  if ((index_names_[table_name].find(index_name) == index_names_[table_name].end())) return DB_INDEX_NOT_FOUND;

  auto index_id = index_names_[table_name][index_name];
  index_names_.erase(table_name);
  indexes_.erase(index_id);
  //注意catalog_meta
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  buffer_pool_manager_->DeletePage(page_id);
  
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  
  Page* meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);//获取页面
  //把数据保存到meta_page
  catalog_meta_->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if (!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

//读入table
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  //已经存在
  if (tables_.find(table_id) != tables_.end()) return DB_TABLE_ALREADY_EXIST;
  //页面
  Page* meta_page = buffer_pool_manager_->FetchPage(page_id);
  //生成table_meta
  TableMetadata *table_meta;
  TableMetadata::DeserializeFrom(meta_page->GetData(), table_meta);
  //生成table_heap table_info
  TableHeap *table_heap=TableHeap::Create(buffer_pool_manager_, page_id, table_meta->GetSchema(), log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  //更新catalog table
  table_names_[table_meta->GetTableName()] = table_id;
  tables_[table_id] = table_info;
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  //检查
  if (indexes_.find(index_id) != indexes_.end()) return DB_INDEX_ALREADY_EXIST;

  Page* meta_page = buffer_pool_manager_->FetchPage(page_id);
  //生成index_meta
  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(meta_page->GetData(), index_meta);

  //table是否存在
  if(tables_.find(index_meta->GetTableId()) == tables_.end())return DB_TABLE_NOT_EXIST;
  //生成index_info
  auto index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
  //更新catalog index
  auto table_name = tables_[index_meta->GetTableId()]->GetTableName();
  index_names_[table_name][index_meta->GetIndexName()] = index_id;
  indexes_[index_id] = index_info;
  catalog_meta_->index_meta_pages_[index_id] = page_id;

  return DB_SUCCESS;
}


dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id)==tables_.end())
    return DB_TABLE_NOT_EXIST;

  table_info = tables_[table_id];
  return DB_SUCCESS;
}