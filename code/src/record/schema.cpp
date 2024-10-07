#include "record/schema.h"


uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t of=0;
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  of+=sizeof(uint32_t);
  //column.size
  MACH_WRITE_TO(uint32_t, buf+of, GetColumnCount());
  of+=sizeof(uint32_t);
  //column
  for(auto &it:columns_){
    of+=it->SerializeTo(buf+of);

  }
  return of;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size=2*sizeof(uint32_t);
  for(auto &it:columns_){
    size+=it->GetSerializedSize();
  }
  return size;

}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t NUM = MACH_READ_FROM(uint32_t, buf);
  //std::cout<<NUM<<" "<<Schema::SCHEMA_MAGIC_NUM<<std::endl;
  ASSERT(NUM == Schema::SCHEMA_MAGIC_NUM, "Schema magic num error.");//验证
  //偏移
  uint32_t of = sizeof(uint32_t);
  //column个数
  uint32_t col_size = MACH_READ_UINT32(buf + of);
  of += sizeof(uint32_t);

  std::vector<Column *> columns;
  for (auto i = 0; i < col_size; i++) {
    Column *col;
    of += Column::DeserializeFrom(buf + of, col);
    columns.push_back(col);
  }
  schema =  new Schema(columns);
  return of;

}