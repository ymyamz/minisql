#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {

  page_ = buffer_pool_manager->FetchPage(current_page_id);
  leaf_page_ = reinterpret_cast<LeafPage *>(page_->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
  page_->RUnlatch();
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return leaf_page_->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  //如果是本页面最后一个且能获得下一页
  if(item_index==leaf_page_->GetSize()-1 && leaf_page_->GetNextPageId()!=INVALID_PAGE_ID ){
    Page* next_page = buffer_pool_manager->FetchPage(leaf_page_->GetNextPageId());
    next_page->RLatch();
    page_->RUnlatch();
    buffer_pool_manager->UnpinPage(page_->GetPageId(), false);
    page_=next_page;
    leaf_page_ = reinterpret_cast<LeafPage *>(page_->GetData());//同时更新页面和叶节点
  }
  else 
  {
    item_index++;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}