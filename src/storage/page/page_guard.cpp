#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // printf("[BasicPageGuard] construct move, page_id: %d\n", that.page_ != nullptr ? that.PageId() : -1);
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.CLear();
}

void BasicPageGuard::CLear() {
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  // printf("!!! in basic Drop !!!\n");
  // printf("[Basic Drop] page_id: %d, is_dirty: %s\n", page_ != nullptr ? page_->GetPageId() : -1,
  //       page_ != nullptr && page_->IsDirty() ? "true" : "false");
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  CLear();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // printf("[BasicPageGuard] assignment move, page_id: %d\n", that.PageId());
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.CLear();
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // printf("[~BasicPageGuard]\n");
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // printf("[ReadPageGuard] construct move\n");
  Drop();
  guard_ = BasicPageGuard(std::move(that.guard_));
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // printf("[ReadPageGuard] assignment move\n");
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  // printf("[ReadPageGuard] Drop\n");
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  // printf("[~ReadPageGuard]\n");
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // printf("[WritePageGuard] construct move\n");
  Drop();
  guard_ = BasicPageGuard(std::move(that.guard_));
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // printf("[WritePageGuard] assignment move\n");
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  // printf("[WritePageGuard] Drop\n");
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  // printf("[~WritePageGuard]\n");
  Drop();
}  // NOLINT

}  // namespace bustub
