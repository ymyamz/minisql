#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}


page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t extent_id=0,max=meta_page->GetExtentNums();
  while(extent_id<max && meta_page->extent_used_page_[extent_id]>=DiskManager::BITMAP_SIZE)
    extent_id++;

  uint32_t MAX_VALID_META_ID = (PAGE_SIZE - 8) / 4 ;//最大分区id
  //分区都满
  if(extent_id==max && extent_id==MAX_VALID_META_ID){
    return INVALID_PAGE_ID;
  }

  char buf[PAGE_SIZE];
  //新建分区
  if(extent_id==max && extent_id<MAX_VALID_META_ID){
    meta_page->num_extents_++;//增加分区
    meta_page->num_allocated_pages_ ++;//分配一个页面
    meta_page->extent_used_page_[extent_id]=1;
    //新建位图页面
    memset(buf, 0, PAGE_SIZE);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
    uint32_t page_id;
    bitmap->AllocatePage(page_id);

    WritePhysicalPage(1 + extent_id*(DiskManager::BITMAP_SIZE + 1), buf);
    return meta_page->num_allocated_pages_ - 1;
  }
  //普通情况
  uint32_t extent_meta_id=1 + extent_id   * (DiskManager::BITMAP_SIZE + 1),page_id;
  ReadPhysicalPage(extent_meta_id, buf);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  bitmap->AllocatePage(page_id);
  meta_page->num_allocated_pages_ ++;//分配一个页面
  meta_page->extent_used_page_[extent_id]++;
  WritePhysicalPage(extent_meta_id, buf);
  return meta_page->num_allocated_pages_ - 1;//??TO UNDERSTAND
}


void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if(IsPageFree(logical_page_id))return;

  char buf[PAGE_SIZE];
  uint32_t extent_id = logical_page_id / DiskManager::BITMAP_SIZE;
  uint32_t extent_meta_id=1 + extent_id   * (DiskManager::BITMAP_SIZE + 1);
  ReadPhysicalPage(extent_meta_id, buf);

  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  bitmap->DeAllocatePage(logical_page_id % DiskManager::BITMAP_SIZE);

  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  meta_page->num_allocated_pages_--;
  meta_page->extent_used_page_[extent_id]--;
  WritePhysicalPage(extent_meta_id, buf);
}


bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char buf[PAGE_SIZE];
  uint32_t i = logical_page_id / DiskManager::BITMAP_SIZE;
  ReadPhysicalPage(1 + i * (DiskManager::BITMAP_SIZE + 1), buf);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  return bitmap->IsPageFree(logical_page_id % DiskManager::BITMAP_SIZE);
}


page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + logical_page_id / DiskManager::BITMAP_SIZE + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
