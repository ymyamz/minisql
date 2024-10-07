#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}


uint32_t Column::SerializeTo(char *buf) const {
  uint32_t of=0;//偏移

  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  of+=sizeof(uint32_t);
  //name_.size()
  MACH_WRITE_TO(uint32_t,buf+of, name_.size());
  of+=sizeof(uint32_t);
  //name_
  MACH_WRITE_STRING(buf+of, name_);
  of+=name_.size()*sizeof(char);
  //type
  MACH_WRITE_TO(TypeId, buf + of, type_);
  of += sizeof(TypeId);
  //len_
  MACH_WRITE_TO(uint32_t,buf+of, len_);
  of+=sizeof(uint32_t);
  //table_ind
  MACH_WRITE_TO(uint32_t, buf+of, table_ind_);
  of+=sizeof(uint32_t);
  //nullable
  MACH_WRITE_TO(bool, buf + of, nullable_);
  of += sizeof(bool);
  //unique
  MACH_WRITE_TO(bool, buf + of, unique_);
  of += sizeof(bool);
  return of;
}

uint32_t Column::GetSerializedSize() const {
  return 4 * sizeof(uint32_t) + name_.size() * sizeof(char) + sizeof(TypeId) + 2 * sizeof(bool);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t of=0;//偏移
  uint32_t NUM=MACH_READ_FROM(uint32_t,buf);
  ASSERT(NUM == COLUMN_MAGIC_NUM, "Column magic num error.");
  of+=sizeof(uint32_t);

  uint32_t name_size,len_,table_ind_;
  //name_size
  name_size=MACH_READ_FROM(uint32_t,buf+of);
  of+=sizeof(uint32_t);
  //name，注意size不包括/0
  char tmp[name_size / sizeof(char) + 1];
  memcpy(tmp, buf + of, name_size);
  tmp[name_size / sizeof(char)]='/0';
  of+=sizeof(char)*name_size;
  //type
  TypeId type_ = MACH_READ_FROM(TypeId, buf + of);
  of += sizeof(TypeId);
  //len_
  len_ = MACH_READ_FROM(uint32_t, buf + of);
  of += sizeof(uint32_t);
  //table_ind
  table_ind_ = MACH_READ_FROM(uint32_t, buf + of);
  of += sizeof(uint32_t);
  //nullable
  bool nullable_ = MACH_READ_FROM(bool, buf + of);
  of += sizeof(bool);
  //unique
  bool unique_ = MACH_READ_FROM(bool, buf + of);
  of += sizeof(bool);
  
  if (type_ != kTypeChar)  
    column = new Column(tmp, type_, table_ind_, nullable_, unique_);
  else
    column = new Column(tmp, type_, len_, table_ind_, nullable_, unique_);

  return of;
}
