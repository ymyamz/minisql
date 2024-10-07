#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {

  leaf_max_size_ = (((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / ( KM.GetKeySize() + sizeof(RowId)) ) - 1);
  internal_max_size_ = (((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / ( KM.GetKeySize() + sizeof(page_id_t)) ) - 1);
  //分配root-index
  auto root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (!root_page->GetRootId(index_id, &this->root_page_id_)) {
    this->root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager->UnpinPage(root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {

}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if(IsEmpty())
    return false;
  
  Page* leaf_page = this->FindLeafPage(key);//注意需要URLatch和unpin
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  RowId tmp;
  if(!leaf_node->Lookup(key, tmp, processor_))//如果找不到
  {
    leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  else 
  {
    leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    result.push_back(tmp);
    return true;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;
  }
  else{

    return InsertIntoLeaf(key, value, transaction);
  }

}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  Page* root_page = buffer_pool_manager_->NewPage(root_page_id_);

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(root_page->GetData());
  leaf_page->Init(root_page_id_,INVALID_PAGE_ID,processor_.GetKeySize(),leaf_max_size_);
  leaf_page->Insert(key, value, processor_);//new node没有溢出问题

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  UpdateRootPageId(1);//插入一条新的

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  //注意插入一定不会是某个叶节点的第一个，所以插入后除非分裂，父节点的key不会变化
  Page* fth_page = this->FindLeafPage(key);//被插入的叶页面
  LeafPage* tmp_leaf_page = reinterpret_cast<LeafPage*>(fth_page->GetData());//被插入的叶节点
  int old_size = tmp_leaf_page->GetSize();
  int new_size = tmp_leaf_page->Insert(key, value, processor_);
  if(new_size==old_size){//重复
    fth_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(fth_page->GetPageId(), false);
    return false;
  }
  else if(new_size < leaf_max_size_){//没有分裂
    fth_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(fth_page->GetPageId(), true);//修改了
    return true;
  }else{
    //分裂
    LeafPage* sibling_leaf_node = Split(tmp_leaf_page,transaction);//注意unpin和WULATCH
    sibling_leaf_node->SetNextPageId(tmp_leaf_page->GetNextPageId());//继承了tmp后半部分的元素
    tmp_leaf_page->SetNextPageId(sibling_leaf_node->GetPageId());
    //处理父节点的key
    GenericKey *new_key = sibling_leaf_node->KeyAt(0);
    InsertIntoParent(tmp_leaf_page, new_key, sibling_leaf_node, transaction);
    fth_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(fth_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);
    return true;
  }

}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  InternalPage* new_node = reinterpret_cast<InternalPage*>(new_page->GetData());

  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(),internal_max_size_);
  node->MoveHalfTo(new_node, buffer_pool_manager_);//此时new_node的keyat(0)!=INVALID，而是保存了VALUE(0)的最小值

  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);//申请新页面
  LeafPage* new_node = reinterpret_cast<LeafPage*>(new_page->GetData());//新页面是叶节点

  new_node->Init(new_page_id,node->GetParentPageId(),node->GetKeySize(),leaf_max_size_);

  node->MoveHalfTo(new_node);//移动后半节点
  return new_node;
}

/* SPLIT处理父节点
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  //如果分裂的是根节点
  if(old_node->IsRootPage()){
    Page* new_page = buffer_pool_manager_->NewPage(root_page_id_);//生成新根节点

    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(root_page_id_,INVALID_PAGE_ID,processor_.GetKeySize(),internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    UpdateRootPageId(0);//更新
    return;
  }                            
  //如果不是根节点,插入在父节点上
  Page* parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int new_size = parent_node->InsertNodeAfter(old_node->GetPageId(),key, new_node->GetPageId());//在old key后加上new key
  if (new_size < internal_max_size_) //中间节点不分裂
  {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }else{
    //父节点分裂
    auto parent_new_sibling_node = Split(parent_node,transaction);
    GenericKey* new_key = parent_new_sibling_node->KeyAt(0);//注意此时KAY(0)保存了VALUE(0)中最小值,即new_node最小值
    InsertIntoParent(parent_node, new_key, parent_new_sibling_node, transaction);

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_new_sibling_node->GetPageId(), true);
  }
  return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if(IsEmpty())
    return;
  Page* leaf_page = FindLeafPage(key);//找到页面
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int org_size = node->GetSize();
  int new_size = node->RemoveAndDeleteRecord(key, processor_);
  if (org_size == new_size) //如果不存在key
  {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  else 
  {
    leaf_page->WUnlatch();
    if(CoalesceOrRedistribute(node, transaction))//如果该node确认要删除
    {
      buffer_pool_manager_->DeletePage(node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  return ;
  
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
//判断合并or分配，返回node是否应该被删除
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  //如果是根节点
  if(node->IsRootPage())
    return AdjustRoot(node);//返回是否应该被删除
  //如果删除后大于等于最小size
  if (node->GetSize() >= node->GetMinSize()) 
    return false;
  //一般情况判断(非根节点删除后小于min_size)
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());//父节点
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  int index = parent_node->ValueIndex(node->GetPageId());//对应index
  int sibling_index;
  if(index == 0)
    sibling_index = 1;
  else 
    sibling_index = index -1;//确保存在兄弟节点

  Page* sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(sibling_index));
  N* sibling_node = reinterpret_cast<N*>(sibling_page->GetData());
  //sibling_page->Wlatch();?
  //重新分配
  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize())
  {
    Redistribute(sibling_node, node, index);

    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

    return false;
  }
  //合并
  else 
  {
    bool if_should_delete = Coalesce(sibling_node, node, parent_node, index);//返回父节点是否应该被删除
    if(if_should_delete)
    {
      buffer_pool_manager_->DeletePage(parent_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  int delete_index= index;
  if (index == 0) 
  {
    delete_index = 1;
    std::swap(node, neighbor_node);
  }//sibling在前 node在后面

  node->MoveAllTo(neighbor_node);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);

  parent->Remove(delete_index);
  return CoalesceOrRedistribute(parent);//递归父亲节点
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  int delete_index= index;
  if (index == 0) 
  {
    delete_index = 1;
    std::swap(node, neighbor_node);
  }//sibling在前 node在后面

  GenericKey* middle_key = parent->KeyAt(delete_index);//node value(0)中的最小值,记录在父节点中
  node->MoveAllTo(neighbor_node,middle_key,buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);

  parent->Remove(delete_index);
  return CoalesceOrRedistribute(parent);//递归父亲节点
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {//index是node的
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  //node在前面，neighbor第一个前移
  if (index == 0) 
  {
    neighbor_node->MoveFirstToEndOf(node);
    parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
  } 
  //node在后，neighbor_node最后一个后移
  else 
  {
    neighbor_node->MoveLastToFrontOf(node);
    parent_node->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);//修改了parent

}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  //node在前面，neighbor第一个前移
  if (index == 0) 
  {
    neighbor_node->MoveFirstToEndOf(node,parent_node->KeyAt(1),buffer_pool_manager_);
    parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
  } 
  //node在后，neighbor_node最后一个后移
  else 
  {
    neighbor_node->MoveLastToFrontOf(node,parent_node->KeyAt(index),buffer_pool_manager_);
    parent_node->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);


}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
   //如果root是中间节点而且只剩下一个孩子
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1)
  {
    InternalPage *root_node = reinterpret_cast<InternalPage*>(old_root_node);
    Page* child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));//唯一的孩子做根节点
    
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
    child_node->SetParentPageId(INVALID_PAGE_ID);
    this->root_page_id_ = child_node->GetPageId();
    UpdateRootPageId(0);//更新
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
    return true;
  }
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){//如果是叶子节点且没有孩子，要删除old_root_node
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page* first_page = FindLeafPage(NULL,true,false);//leftmost
  return IndexIterator(first_page->GetPageId(),buffer_pool_manager_, 0);

}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page* found_page = FindLeafPage(key);
  LeafPage* found_node = reinterpret_cast<LeafPage*>(found_page->GetData());
  int index = found_node->KeyIndex(key, processor_);
  return IndexIterator(found_page->GetPageId(),buffer_pool_manager_,index);
}

/*最后一个index的下一个
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  Page* end_page = FindLeafPage(NULL,false,true);//rightmost
  LeafPage* end_node = reinterpret_cast<LeafPage*>(end_page->GetData());
  int index = end_node->GetSize();
  return IndexIterator(end_page->GetPageId(),buffer_pool_manager_,index);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/* 注意有Rlatch和PIN未接触
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, bool leftMost,bool rightMost) {
  Page* fth_page = buffer_pool_manager_->FetchPage(root_page_id_);//从page_id开始查找
  BPlusTreePage* tmp_node = reinterpret_cast<BPlusTreePage *>(fth_page->GetData());
  fth_page->RLatch();

  while (!tmp_node->IsLeafPage()) 
  {
    InternalPage* internal_node = reinterpret_cast<InternalPage *>(tmp_node);
    page_id_t child_page_id;
    if (leftMost) 
    {
      child_page_id = internal_node->ValueAt(0);
    }else if(rightMost){
      child_page_id = internal_node->ValueAt(internal_node->GetSize()-1);
    }
    else 
    {
      child_page_id = internal_node->Lookup(key,processor_);
    }

    Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
    BPlusTreePage* child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    child_page->RLatch();
    fth_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(fth_page->GetPageId(), false);

    fth_page = child_page;
    tmp_node = child_node;
  }

  return fth_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  IndexRootsPage *tmp = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));

  if (insert_record != 0) {//1插入 0更新
    tmp->Insert(index_id_, root_page_id_);
  } else {
    tmp->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}