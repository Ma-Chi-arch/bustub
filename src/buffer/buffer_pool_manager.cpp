//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

auto GetThreadId() -> std::string {
  // 获取当前线程号
  pthread_t thread_id = pthread_self();

  auto long_thread_id = reinterpret_cast<uint64_t>(thread_id);

  return std::to_string(long_thread_id);
}

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  // printf("[BufferPoolManager] pool_size: %zu, replacer_k: %zu\n", pool_size, replacer_k);
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  // printf("[NewPage] %lu %zu %lu %lu\n", free_list_.size(), replacer_->Size(), pool_size_, replacer_->Size());
  if (page_table_.size() == pool_size_ && replacer_->Size() == 0U) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto exist = replacer_->Evict(&frame_id);
    if (!exist) {
      throw ExecutionException("[BufferPoolManager::NewPage] impossible, must have free frame !");
    }
    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].page_id_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
  }
  page_id_t aloc_page_id = AllocatePage();
  page_table_[aloc_page_id] = frame_id;
  pages_[frame_id].page_id_ = aloc_page_id;
  pages_[frame_id].pin_count_ = 1;
  *page_id = aloc_page_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  // printf("[NewPage] project %d -> %d\n", aloc_page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  // printf("[FetchPage] %lu %zu %lu %d\n", free_list_.size(), replacer_->Size(), pool_size_, page_id);
  if (page_table_.count(page_id) != 0U) {
    pages_[page_table_[page_id]].pin_count_++;
    replacer_->SetEvictable(page_table_[page_id], false);
    return &pages_[page_table_[page_id]];
  }
  if ((page_table_.size() == pool_size_ && replacer_->Size() == 0U) || page_id < 0) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto exist = replacer_->Evict(&frame_id);
    if (!exist) {
      throw ExecutionException("[BufferPoolManager::FetchPage] impossible, must have free frame !");
    }
    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].page_id_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
  }
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  // printf("[FetchPage] project %d -> %d\n", page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  // printf("before UnpinPage: page_id: %d, replacer size: %zu, dirty: %s\n", page_id, replacer_->Size(),
  //       is_dirty ? "true" : "false");
  if (page_table_.count(page_id) == 0U || pages_[page_table_[page_id]].pin_count_ <= 0) {
    // printf("--- here ---\n");
    return false;
  }
  pages_[page_table_[page_id]].pin_count_--;
  // printf("pin_count: %d\n", pages_[page_table_[page_id]].pin_count_);
  if (pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  pages_[page_table_[page_id]].is_dirty_ = pages_[page_table_[page_id]].is_dirty_ || is_dirty;
  // printf("after UnpinPage: replacer size: %zu\n", replacer_->Size());
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  // printf("[FlushPage] page_id: %d, id_dirty: %s\n", page_id, pages_[page_table_[page_id]].is_dirty_ ?
  // "true" : "false");
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
  pages_[page_table_[page_id]].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  // printf("FlushAllPages\n");
  for (auto [page_id, frame_id] : page_table_) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  // printf("[DeletePage] page_id: %d\n", page_id);
  if (page_table_.count(page_id) != 0U && pages_[page_table_[page_id]].pin_count_ > 0) {
    return false;
  }
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  replacer_->Remove(page_table_[page_id]);
  free_list_.push_back(page_table_[page_id]);
  pages_[page_table_[page_id]].ResetMemory();
  pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
  pages_[page_table_[page_id]].pin_count_ = 0;
  pages_[page_table_[page_id]].is_dirty_ = false;
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // printf("[BufferPoolManager] FetchPageBasic\n");
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // printf("[BufferPoolManager] FetchPageRead\n");
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // printf("[BufferPoolManager] FetchPageWrite\n");
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // printf("[BufferPoolManager] NewPageGuarded\n");
  return {this, NewPage(page_id)};
}

}  // namespace bustub
