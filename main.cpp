#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

struct task_t {
  void operator()() const;
};

struct th_pool_t {
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  template< class F >
  auto submit(F && f) -> std::future< decltype(f()) >;

  void wait_all();

private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable done_cd_;
  bool stop_{false};
  size_t unfinished_{0};
  std::vector< std::thread > ths_;
};

void task_t::operator()() const
{
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  std::cout << "!\n";
}

template< class F >
auto th_pool_t::submit(F && f) -> std::future< decltype(f()) >
{
  using res_t = decltype(f());
  auto task = std::make_shared< std::packaged_task< res_t() > >(std::forward< F >(f));
  std::future< res_t > res = task->get_future();
  {
    std::lock_guard< std::mutex > lck(mx_);
    tasks_.push([task]() {
      (*task)();
    });
    ++unfinished_;
  }
  cd_.notify_one();
  return res;
}

void th_pool_t::wait_all()
{
  std::unique_lock< std::mutex > lck(mx_);
  auto all_done = [&]() {
    return unfinished_ == 0;
  };
  done_cd_.wait(lck, all_done);
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() {
    execute();
  };
  for (; k; --k) {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  mx_.unlock();
  cd_.notify_all();
  for (auto && th : ths_) {
    th.join();
  }
}

void th_pool_t::execute()
{
  std::unique_lock< std::mutex > lck(mx_);
  do {
    auto proceed = [&]() {
      return !tasks_.empty() || stop_;
    };
    cd_.wait(lck, proceed);
    if (tasks_.empty()) {
      continue;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try {
      task();
    } catch (...) {
    }
    lck.lock();
    --unfinished_;
    if (unfinished_ == 0) {
      done_cd_.notify_all();
    }
  } while (!stop_ || !tasks_.empty());
}

int main()
{
  th_pool_t pool(5);

  for (size_t i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }
  pool.wait_all();
  std::cout << "wait_all: 10 tasks done\n";

  auto sum = pool.submit([]() {
    int s = 0;
    for (int i = 1; i <= 100; ++i) {
      s += i;
    }
    return s;
  });
  auto fail = pool.submit([]() -> int {
    throw std::runtime_error("error inside task");
  });

  std::cout << "sum = " << sum.get() << '\n';
  try {
    fail.get();
  } catch (const std::exception & e) {
    std::cout << "caught: " << e.what() << '\n';
  }
}
