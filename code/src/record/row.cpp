#include "record/row.h"


uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t of=0;//偏移

  //field num
  MACH_WRITE_TO(uint32_t, buf, fields_.size());
  of+=sizeof(uint32_t);
  //null num
  MACH_WRITE_TO(uint32_t, buf+of, null_nums);
  of+=sizeof(uint32_t);
  //field空记录
  for(uint32_t i=0;i<fields_.size();i++){
    if(fields_[i]->IsNull()){
      MACH_WRITE_TO(uint32_t, buf, i);
      of+=sizeof(uint32_t);
    }
  }
  //field非空
  for(auto &it:fields_){
    if(!it->IsNull())
      of+=it->SerializeTo(buf+of);
  }
  
  return of;

}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  
  uint32_t of=0;//偏移
  uint32_t i,num,null,temp;
  //total
  num=MACH_READ_FROM(uint32_t,buf);
  of+=sizeof(uint32_t);
  //null
  null=MACH_READ_FROM(uint32_t,buf+of);
  of+=sizeof(uint32_t);
  uint32_t *bitset=new uint32_t[num];
  for(i=0;i<num;i++)bitset[i]=0;
  //null_set
  for(i=0;i<null;i++){
    temp=MACH_READ_FROM(uint32_t,buf+of);
    bitset[temp]=1;
  }
  //field loading
  for(i=0;i<num;i++){
    TypeId field_type=schema->GetColumn(i)->GetType();//类型
    Field* temp = new Field(field_type);
    fields_.push_back(temp);
    if(bitset[i]==0){//非空
      of += Field::DeserializeFrom(buf+of,field_type,&fields_[i],false);
    }
  }
  return of;

}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  if(fields_.size() == 0)
    return 0;
  uint32_t size=(2+null_nums)*sizeof(u_int32_t);//num,null
  for(auto &it:fields_){
    if(!it->IsNull())
      size+=it->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
     
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
