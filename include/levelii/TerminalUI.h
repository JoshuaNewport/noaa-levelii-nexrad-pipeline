#pragma once

#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

class BackgroundFrameFetcher;

class TerminalUI {
public:
    explicit TerminalUI(std::shared_ptr<BackgroundFrameFetcher> fetcher);
    
    void render();
    void clear_screen();

private:
    std::shared_ptr<BackgroundFrameFetcher> fetcher_;
    
    std::string format_size(uint64_t bytes);
    std::string format_time(uint64_t timestamp);
    std::string format_time_short(uint64_t timestamp);
    std::string get_status_color(bool is_running);
};
