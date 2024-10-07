#include "page/bitmap_page.h"

#include "glog/logging.h"


template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ >=GetMaxSupportedSize()){
    return false;
  }
  if(!bytes.test(next_free_page_ )){
    bytes.set(next_free_page_ ,1);
    page_offset=next_free_page_;
    page_allocated_ ++;
    for(next_free_page_=0;next_free_page_<GetMaxSupportedSize();next_free_page_++){
      if(!bytes.test(next_free_page_)) break;
    }
    return true;
  }
  
  return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {

  if(bytes.test(page_offset)){
    bytes.set(page_offset, 0);

    if(page_offset < next_free_page_) 
      next_free_page_ = page_offset;
    page_allocated_--;
    return true;
  }else
    return false;
}


template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return !bytes.test(page_offset);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
