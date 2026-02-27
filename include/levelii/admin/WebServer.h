/**
 * WebServer.h - Pure C++ HTTP server abstraction
 * 
 * Provides a lightweight wrapper around httplib for serving HTTP endpoints.
 * Hides implementation details and provides a clean interface for route registration.
 */

#pragma once

#include <string>
#include <functional>
#include <memory>

class WebServer {
public:
    using RequestHandler = std::function<std::string(const std::string& body, const std::string& param)>;

    explicit WebServer(const std::string& host = "localhost", int port = 13480);
    ~WebServer();

    void add_route(const std::string& method, const std::string& path,
                   RequestHandler handler);
    
    void start();
    void stop();
    bool is_running() const;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
    
    std::string host_;
    int port_;
};
