#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include <unordered_map>
using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 * LRUReplacer的大小默认与Buffer Pool的大小相同。
 */
struct ListNode{
  ListNode* next;
  ListNode* prev;//链表的下个点和上一个点
  frame_id_t frame_id;//储存的frame_id
};

class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  unordered_map<frame_id_t, ListNode*> Hash;//unordered_map记录frame中的位置
  ListNode *Head, *Tail;
  size_t size;
  size_t MAX_SIZE;//缓冲区最大容量
  // add your own private member variables here
};

struct ClockListNode{
  ClockListNode* next;
  ClockListNode* prev;//链表的下个点和上一个点
  frame_id_t frame_id;//储存的frame_id
  bool usebit;//是否使用
};

class ClockReplacer : public Replacer{
  public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  size_t size;
  unordered_map<frame_id_t, ClockListNode*> Hash;
  ClockListNode* Head;//frame_id + use bit
  ClockListNode* index;
};

#endif  // MINISQL_LRU_REPLACER_H
