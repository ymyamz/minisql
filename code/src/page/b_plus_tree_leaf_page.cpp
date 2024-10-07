#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int i;
  for(i=0; i<this->GetSize(); i++)
  {
    if(KM.CompareKeys(KeyAt(i),key)>=0)
      return i;
  }
  return i;//如果i==GetSize(),那么key大于里面所有值
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(KeyAt(index),ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  //std::cout<<"insert to leaf"<<std::endl;
  int key_index = this->KeyIndex(key, KM);//查找第一个大于key的
  //std::cout<<"key_index="<<key_index<<" total_size="<<this->GetSize()<<std::endl;
  if(key_index == this->GetSize())//没有大于key的
  {
    SetKeyAt(key_index,key);
    SetValueAt(key_index,value);
    this->IncreaseSize(1);
    return this->GetSize();
  }
  if(KM.CompareKeys(KeyAt(key_index), key) == 0)//如果有一个同样key存在
  {
    return this->GetSize();
  }

  for(int i=GetSize();i>key_index;i--){
    SetKeyAt(i,KeyAt(i-1));
    SetValueAt(i,ValueAt(i-1));
  }
  SetKeyAt(key_index,key);
  SetValueAt(key_index,value);

  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * 把后半部分移动到接受节点
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int start_index = this->GetMinSize();
  this->SetSize(start_index);
  recipient->CopyNFrom(pairs_off + start_index * pair_size, GetMaxSize()-start_index);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  PairCopy(pairs_off + GetSize() * pair_size,src,size);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int key_index=KeyIndex(key,KM);
  if(key_index == this->GetSize())
  {
    return false;
  }
  else if(KM.CompareKeys(this->KeyAt(key_index), key) != 0)
  {
    return false;
  }
  else 
  {
    value = ValueAt(key_index); 
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int key_index = this->KeyIndex(key, KM);
  if(key_index == this->GetSize())
  {
    return this->GetSize();
  }
  else if(KM.CompareKeys(this->KeyAt(key_index), key)!=0)
  {
    return this->GetSize();
  }
  else 
  {
    std::move(pairs_off + (key_index+1) * pair_size, pairs_off + GetSize() * pair_size, pairs_off + key_index * pair_size);
    this->IncreaseSize(-1);
    return this->GetSize();
  }

  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(pairs_off, this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {  
  recipient->CopyLastFrom(KeyAt(0),ValueAt(0));
  std::move(pairs_off + pair_size, pairs_off + GetSize() * pair_size, pairs_off);
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(),key);
  SetValueAt(GetSize(),value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(KeyAt(GetSize()-1),ValueAt(GetSize()-1));
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  std::move(pairs_off, pairs_off + GetSize() * pair_size, pairs_off + pair_size);//前面空出一个位置
  SetKeyAt(0,key);
  SetValueAt(0,value);
  IncreaseSize(1);
}
