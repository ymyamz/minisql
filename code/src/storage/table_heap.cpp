#include "storage/table_heap.h"

//插入后，更新row的rid
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW)//如果太大
    return false;
  //第一页不存在
  TablePage* page;
  if(first_page_id_==INVALID_PAGE_ID){
    page = (TablePage *)buffer_pool_manager_->NewPage(first_page_id_);//new page
    ASSERT(page != nullptr, "Table heap can not initialize the first page.");
    page->Init(first_page_id_,INVALID_PAGE_ID,log_manager_,txn);
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
  }else{
    page = (TablePage *)buffer_pool_manager_->FetchPage(first_page_id_);//获取第一页
    //fetch error
    ASSERT(page != nullptr, "Can not fetch the first page for table heap.");
  }
  page_id_t next_page_id;
  page_id_t cur_page_id=first_page_id_;
  //insert tuple.如果page无法插入，则一直向下寻找有空的页面
  while( !page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_) ){
    next_page_id=page->GetNextPageId();
    if(next_page_id!=INVALID_PAGE_ID){//下一页存在
      page = (TablePage *)buffer_pool_manager_->FetchPage(next_page_id);
      buffer_pool_manager_->UnpinPage(cur_page_id, false);//取消固定
      cur_page_id=next_page_id;
    }else{//下一页不存在,则获取新页面，扩展链表
      TablePage* next_page=(TablePage *)buffer_pool_manager_->NewPage(next_page_id);//获取一个新页面
      if(next_page_id!=INVALID_PAGE_ID) {//成功获得新页面
        next_page->Init(next_page_id, page->GetTablePageId(), log_manager_, txn);//初始化
        page->SetNextPageId(next_page_id);
        next_page->SetPrevPageId(cur_page_id);

        buffer_pool_manager_->UnpinPage(cur_page_id, true);//cur_page更新了next_page
        page=next_page;
        cur_page_id=next_page_id;
      }else{//获取新页面失败
        buffer_pool_manager_->UnpinPage(cur_page_id, false);
        return false;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(cur_page_id, true);//更新了
  return true;

}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}


bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page == nullptr) return false;
  Row old_row_(rid);
  auto re_status=page->UpdateTuple(row, &old_row_, schema_, txn, lock_manager_, log_manager_);
  switch (re_status)
  {
  case 1://成功
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;

  case 2://不存在
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;

  case 3://如果被删除
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;

  case 4://如果空间不够,先删除，再插入
    MarkDelete(rid, txn);
    InsertTuple(old_row_, txn);
    buffer_pool_manager_->UnpinPage(old_row_.GetRowId().GetPageId(), true);
    return true;

  }
  return false;
}


void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId rid=row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page==nullptr)return false;
  return page->GetTuple(row,schema_,txn,lock_manager_);
  
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}


TableIterator TableHeap::Begin(Transaction *txn) {
  RowId first_rid=INVALID_ROWID;//如果不存在，返回invalid
  TablePage *page;
  page_id_t page_id=first_page_id_;
  while(page_id!=INVALID_PAGE_ID){
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    //如果找到
    if(page->GetFirstTupleRid(&first_rid)){
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      break;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page_id=page->GetNextPageId();
  }
  return TableIterator(this,first_rid,txn);

}


TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);
}
