// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    // 计算出每个类型的CRC32值
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) {
  // 直接拿原始指针
  const char* ptr = slice.data();
  // 取得数据长度
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    // 计算一个块还剩下多少空间
    // 每一个块32KB
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // 如果块空间都不够数据头了
    if (leftover < kHeaderSize) {
      // Switch to a new block
      // 如果块剩余的数量不为0
      // 直接写入\x00做trailer
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // 块偏移直接设置为0
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
    // 是否还有足够的数据空间
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 获取该分片大小
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 是否文件到底了
    const bool end = (left == fragment_length);
    if (begin && end) {
      // 一次性写满
      type = kFullType;
    } else if (begin) {
      // 第一个块
      type = kFirstType;
    } else if (end) {
      // 最后一个块
      type = kLastType;
    } else {
      // 中间块
      type = kMiddleType;
    }
    // 提交物理纪录
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    // 移动数据的指针
    ptr += fragment_length;
    // 计算余量
    left -= fragment_length;
    begin = false;
    // 如果还有余量，说明这个日志很大，一个块无法全部存满
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + (int)n <= kBlockSize);

  // Format the header
  // 创建block的头
  char buf[kHeaderSize];
  // 0，1，2，3这三个位置都为0
  // 4为大小的低8位，5为大小的高8位，小端序列
  // 6为日志类型
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // 计算出crc
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  // 填充到buf的头部
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 向日志文件写入块头
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    // 写入数据
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  // 增加块的offset
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
