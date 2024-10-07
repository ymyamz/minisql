#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"


TableIterator::TableIterator(TableHeap *t,RowId id,Transaction *txn):table_heap_(t),txn_(txn){
  row_=new Row(id);//将rowid类型设为row,利用gettuple函数获取row
  if(id.GetPageId()!=INVALID_PAGE_ID)
    table_heap_->GetTuple(row_,txn);
  
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_=other.table_heap_;
  row_=new Row(*other.row_);//复制
  txn_=other.txn_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return itr.row_->GetRowId()==row_->GetRowId();//对RowId进行==判断
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(itr==*this);
}

const Row &TableIterator::operator*() {

  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_=itr.table_heap_;
  row_=new Row(*itr.row_);//复制
  txn_=itr.txn_;;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page_id=row_->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  page->RLatch();//读lock

  RowId next_row_;
  if(!page->GetNextTupleRid(row_->GetRowId(), &next_row_)){//如果不能获得下一个，说明在page末尾

    while(page->GetNextPageId() != INVALID_PAGE_ID){//找到不为空的
      auto next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);//fetch后要unpin
      page->RUnlatch();//把上一个页面阅读lock解除
      page = next_page;//进入下一页
      page->RLatch();
      if(page->GetFirstTupleRid(&next_row_))//如果能读到row，跳出；否则，进入下一页
        break;
    }
    
  }
  row_ = new Row(next_row_);
  if(*this != table_heap_->End()){
    table_heap_->GetTuple(row_, txn_);
  }
  page->RUnlatch();//unlock
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);//fetch后要unpin,未修改
  return *this;
} 


// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator clone(*this);
  ++(*this);
  return clone;
}
