//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

// NOLINTNEXTLINE
TEST(PageGuardTest, DISABLED_MyTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  {
    auto guard = bpm->FetchPageWrite(0);
    printf("!!!! 1\n");
    auto guard1 = bpm->FetchPageWrite(0);
    auto guard2 = bpm->FetchPageWrite(0);
    printf("!!!! 2\n");
  }
  {
    auto guarded_page0 = WritePageGuard(bpm.get(), page0);

    // auto guarded_page0_copy = std::move(guarded_page0);

    guarded_page0.Drop();

    EXPECT_EQ(0, page0->GetPinCount());
  }
  auto page1 = bpm->NewPage(&page_id_temp);
  {
    auto guard_page0_read = ReadPageGuard(bpm.get(), page0);
    auto guard_page1_write = WritePageGuard(bpm.get(), page1);

    guard_page0_read.Drop();

    guard_page1_write.Drop();

    // auto guarded_page0_copy = std::move(guarded_page0);

    EXPECT_EQ(0, page0->GetPinCount());
    EXPECT_EQ(0, page1->GetPinCount());
  }
  printf("! here \n");
  EXPECT_EQ(0, page0->GetPinCount());

  // auto guard = bpm->FetchPageRead(page_id_temp);
  // guard.Drop();

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, ReadTset) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // test ~ReadPageGuard()
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test ReadPageGuard(ReadPageGuard &&that)
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = ReadPageGuard(std::move(reader_guard));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test ReadPageGuard::operator=(ReadPageGuard &&that)
  {
    auto reader_guard_1 = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_1 = std::move(reader_guard_2);
    EXPECT_EQ(2, page0->GetPinCount());
  }

  // test ReadPageGuard::operator=(ReadPageGuard &&that)
  {
    // auto reader_guard_1 = bpm->FetchPageWrite(page_id_temp);
    // auto reader_guard_2 = bpm->FetchPageWrite(page_id_temp);
    // EXPECT_EQ(3, page0->GetPinCount());
    // reader_guard_1 = std::move(reader_guard_2);
    // EXPECT_EQ(2, page0->GetPinCount());
  }

  EXPECT_EQ(1, page0->GetPinCount());

  bpm->UnpinPage(page0->GetPageId(), false);

  EXPECT_EQ(0, page0->GetPinCount());

  page0 = bpm->FetchPage(0);
  EXPECT_EQ(1, page0->GetPinCount());
  {
    auto guard = bpm->FetchPageRead(0);
    EXPECT_EQ(2, page0->GetPinCount());
    auto guard2 = bpm->FetchPageWrite(0);
    // auto guard3 = bpm->FetchPageWrite(0);
    // printf("!!!!! %d\n", page0->GetPinCount());
    EXPECT_EQ(3, page0->GetPinCount());
  }

  {
    auto guard = bpm->FetchPageWrite(0);
    auto guard1 = bpm->FetchPageWrite(0);
    auto guard2 = bpm->FetchPageWrite(0);
    auto guard3 = bpm->FetchPageWrite(0);
  }

  EXPECT_EQ(1, page0->GetPinCount());
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, HHTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp = 0;
  page_id_t page_id_temp_a;
  auto *page0 = bpm->NewPage(&page_id_temp);
  auto *page1 = bpm->NewPage(&page_id_temp_a);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page_a = BasicPageGuard(bpm.get(), page1);

  // after drop, whether destructor decrements the pin_count_ ?
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard1.Drop();
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(1, page1->GetPinCount());
  // test the move assignment
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard2 = std::move(read_guard1);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test the move constructor
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2(std::move(read_guard1));
    auto read_guard3(std::move(read_guard2));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page_id_temp, page0->GetPageId());

  // repeat drop
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());

  disk_manager->ShutDown();
}

}  // namespace bustub
