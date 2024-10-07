#include "buffer/lru_replacer.h"
#include <iostream>
#include <assert.h>
//内存并没有进行立刻的释放
using namespace std;
LRUReplacer::LRUReplacer(size_t num_pages){
  MAX_SIZE = num_pages;
  Head = Tail = new ListNode();//初始化list, using dummy node
  Head->next = Head->prev = nullptr;
  Head->frame_id = -1;//不存在的frame_id
  size = 0;//初始化size
  Hash.clear();//初始化HashMap
}

LRUReplacer::~LRUReplacer(){//释放空间
  while(Head != Tail){
    ListNode* node = Head;
    Head = Head->next;
    delete node;
  }//释放内存
  delete Head;//释放最后一个节点
}

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // return false;
  if(!size) return false;//if no size, then fail
  ListNode* node = Head->next;//删除最前面的点
  assert(node != nullptr);
  Head->next = node->next;
  if(node->next)
    node->next->prev = Head;
  if(node == Tail){
    Tail = Head; 
    // Head->next = nullptr;
    assert(size == 1);
    assert(Tail->next == nullptr);
  }
  *frame_id = node->frame_id;
  Hash.erase(*frame_id);
  delete node;//回收node的空间
  size--;
  // puts("Victim");
  // for(auto x = Head;; x = x->next){
  //   cout << x << ' ';
  //   if(x == Tail) break;
  // }
  // puts("");
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  // puts("pin1");
  if(!Hash.count(frame_id)) return;//如果不在其中就不做任何作用
  ListNode* node = Hash[frame_id];
  // puts("pin2");
  // cout << "Head = " << Head << endl;
  // cout << "prev = " << node->prev << endl;
  // cout << "next = " << node->next << endl;
  if(node->prev)
    node->prev->next = node->next;//更新prev的next
  if(node->next)
    node->next->prev = node->prev;//更新next的prev
  // puts("pin4");
  if(node == Tail){//tail需要注意
    Tail = Tail->prev;
    Tail->next = nullptr;
  }
  Hash.erase(frame_id);//update hashmap
  delete node;//节点删除后回归内存
  size--;//update size;
  // puts("Pin");
  // for(auto x = Head;; x = x->next){
  //   cout << x << ' ';
  //   if(x == Tail) break;
  // }
  // puts("");
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(Hash.count(frame_id)) return;//如果已经在其中就不做任何操作
  if(size == MAX_SIZE) return;//如果超出数量了就不做任何操作？，好像完全不会超过
  Tail->next = new ListNode();//创建新的节点
  Tail->next->prev = Tail;//建立关系
  Tail = Tail->next;//更新Tail
  Tail->next = nullptr;
  Tail->frame_id = frame_id;//store frame_id
  Hash[frame_id] = Tail;//记录现在的指针
  size++;//update size
  // puts("Unpin");
  // for(auto x = Head;; x = x->next){
  //   cout << x << ' ';
  //   if(x == Tail) break;
  // }
  // puts("");
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return size;
}
//clockreplacer
ClockReplacer::ClockReplacer(size_t num_pages){
  size = 0;
  Hash.clear();
  Head = new ClockListNode();//dummy node
  Head->next = Head->prev = nullptr;
  index = nullptr;
}

ClockReplacer::~ClockReplacer(){
  ClockListNode *node = Head->next;
  delete Head;
  while(size){//删除掉所有的点
    Head = node;
    node = node->next;
    delete Head;
    size--; 
  }
}

bool ClockReplacer::Victim(frame_id_t *frame_id){
  if(!size) return false;
  while(index->usebit){
    index->usebit ^= 1;//找到一个可以替换的点
    index = index->next;//不断找下一个
  }
  *frame_id = index->frame_id;
  Hash.erase(*frame_id);
  size--;
  ClockListNode* DeleteNode = nullptr;
  // cout << size << endl;
  if(size){
    index->prev->next = index->next;
    index->next->prev = index->prev;
    DeleteNode = index;
    index = index->next;
  }else{
    DeleteNode = index;
    index = nullptr;
  }
  delete DeleteNode;
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id){
  if(!Hash.count(frame_id)) return;//如果不在其中就不做任何作用
  ClockListNode* node = Hash[frame_id];
  Hash.erase(node->frame_id);
  size--;
  ClockListNode* DeleteNode = nullptr;
  if(size){
    node->prev->next = node->next;
    node->next->prev = node->prev;
    DeleteNode = node;
    if(index == node) index = node->next;//相等时需要移动index
  }else{
    DeleteNode = node;
    index = nullptr;
  }
  delete DeleteNode;
}

void ClockReplacer::Unpin(frame_id_t frame_id){
  if(Hash.count(frame_id)){ 
    Hash[frame_id]->usebit = 1;
    return;//如果在其中就设置一下使用位
  }
  ClockListNode* newnode = new ClockListNode();
  newnode->frame_id = frame_id;
  newnode->usebit = true;//刚刚使用过
  if(!size){
    Head->next = newnode;
    Head->next->prev = Head->next;
    Head->next->next = Head->next;//自己的前后都是自己
    index = newnode;//初始化index
  }
  else{//设置newnode为index的前一个点
    newnode->next = index;//下一个点
    newnode->prev = index->prev;//最后一个点
    newnode->next->prev = newnode;
    newnode->prev->next = newnode;
    Head->next = newnode;//change the first point
  }
  Hash[frame_id] = Head->next;
  size++;
}

size_t ClockReplacer::Size() {
  return size;
}