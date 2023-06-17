//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

size_t time_base_line = 0;

auto CurrentTimestamp() -> size_t {
  ++time_base_line;
  return time_base_line;
}

const size_t INF_DISTANCE = std::numeric_limits<size_t>::max();

auto LRUKNode::GetDistance() const -> size_t {
  if (history_.size() < k_) {
    return INF_DISTANCE;
  }
  return 0;
}

auto LRUKNode::FirstTime() const -> size_t {
  if (history_.empty()) {
    throw ExecutionException("[LRUKNode] history size is empty!");
  }
  return history_.front();
}

auto LRUKNode::Add() -> void {
  auto timestamp = CurrentTimestamp();
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}

auto LRUKNode::SetEvictable(bool is_evictable) -> void { is_evictable_ = is_evictable; }

auto LRUKNode::GetEvictable() const -> bool { return is_evictable_; }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (!st_.empty()) {
    auto [var1, var2, the_frame_id] = *(--st_.end());
    st_.erase(--st_.end());
    *frame_id = the_frame_id;
    node_store_.erase(the_frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw ExecutionException("[replacer] frame id larger than replacer size");
  }
  if (node_store_.count(frame_id) == 0U) {
    node_store_.insert(std::make_pair(frame_id, LRUKNode(frame_id, k_)));
  } else {
    auto tp = std::make_tuple(node_store_[frame_id].GetDistance(), node_store_[frame_id].FirstTime(), frame_id);
    st_.erase(tp);
  }
  node_store_[frame_id].Add();
  if (node_store_[frame_id].GetEvictable()) {
    st_.emplace(node_store_[frame_id].GetDistance(), node_store_[frame_id].FirstTime(), frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  // printf("before SetEvictable frame_id: %d, size: %zu\n", frame_id, st_.size());
  // printf("%d %d\n", node_store_[frame_id].GetEvictable(), set_evictable);
  if (node_store_.count(frame_id) == 0U || node_store_[frame_id].GetEvictable() == set_evictable) {
    return;
  }
  auto tp = std::make_tuple(node_store_[frame_id].GetDistance(), node_store_[frame_id].FirstTime(), frame_id);
  node_store_[frame_id].SetEvictable(set_evictable);
  if (set_evictable) {
    st_.insert(tp);
  } else {
    st_.erase(tp);
  }
  // printf("after SetEvictable frame_id: %d, size: %zu\n", frame_id, st_.size());
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0U) {
    return;
  }
  auto is_evictable = node_store_[frame_id].GetEvictable();
  if (is_evictable) {
    auto tp = std::make_tuple(node_store_[frame_id].GetDistance(), node_store_[frame_id].FirstTime(), frame_id);
    st_.erase(tp);
    node_store_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  return st_.size();
}

}  // namespace bustub
