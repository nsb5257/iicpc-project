#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <shared_mutex>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <sstream>

// In-memory state representation
struct Order {
    double price;
    int quantity;
    std::string side;
    std::string type;
};

// Global Thread-Safe Order Book
std::unordered_map<std::string, Order> order_book;
std::shared_mutex book_mutex;

// Global Thread Pool state
std::queue<int> client_queue;
std::mutex queue_mutex;
std::condition_variable condition;
bool stop_pool = false;

// Lightweight JSON string extractor
std::string get_json_string(const std::string& json, const std::string& key) {
    std::string pattern = "\"" + key + "\":\"";
    size_t start = json.find(pattern);
    if (start != std::string::npos) {
        start += pattern.length();
        size_t end = json.find("\"", start);
        if (end != std::string::npos) {
            return json.substr(start, end - start);
        }
    }
    return "";
}

// Lightweight JSON number extractor
double get_json_number(const std::string& json, const std::string& key) {
    std::string pattern = "\"" + key + "\":";
    size_t start = json.find(pattern);
    if (start != std::string::npos) {
        start += pattern.length();
        size_t end = start;
        while (end < json.length() && (isdigit(json[end]) || json[end] == '.' || json[end] == '-')) {
            end++;
        }
        try {
            return std::stod(json.substr(start, end - start));
        } catch (...) {
            return 0.0;
        }
    }
    return 0.0;
}

// Client request processor
void handle_client(int client_socket) {
    char buffer[2048] = {0};
    ssize_t bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
    
    if (bytes_read <= 0) {
        close(client_socket);
        return;
    }

    std::string request(buffer);
    size_t body_pos = request.find("\r\n\r\n");
    std::string body = (body_pos != std::string::npos) ? request.substr(body_pos + 4) : "";

    std::string response_body;

    if (request.find("POST /order") == 0) {
        std::string id = get_json_string(body, "id");
        std::string type = get_json_string(body, "type");
        std::string side = get_json_string(body, "side");
        double price = get_json_number(body, "price");
        int quantity = static_cast<int>(get_json_number(body, "quantity"));

        // Safely insert into the order book
        {
            std::unique_lock<std::shared_mutex> lock(book_mutex);
            order_book[id] = {price, quantity, side, type};
        }

        double exec_price = (type == "MARKET") ? 100.0 : price;

        response_body = "{\"order_id\":\"" + id + "\",\"type\":\"" + type + "\",\"side\":\"" + side + 
                        "\",\"ordered_qty\":" + std::to_string(quantity) + ",\"filled_qty\":" + std::to_string(quantity) + 
                        ",\"price\":" + std::to_string(price) + ",\"execution_price\":" + std::to_string(exec_price) + "}";

    } else if (request.find("POST /cancel") == 0) {
        std::string id = get_json_string(body, "id");
        std::string target_id = get_json_string(body, "cancel_target_id");

        // Safely remove from the order book
        {
            std::unique_lock<std::shared_mutex> lock(book_mutex);
            order_book.erase(target_id);
        }

        response_body = "{\"order_id\":\"" + id + "\",\"type\":\"CANCEL\",\"status\":\"cancelled\"}";
    } else {
        std::string not_found = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        write(client_socket, not_found.c_str(), not_found.length());
        close(client_socket);
        return;
    }

    std::string http_response = "HTTP/1.1 200 OK\r\n"
                                "Content-Type: application/json\r\n"
                                "Content-Length: " + std::to_string(response_body.length()) + "\r\n"
                                "Connection: close\r\n\r\n" + response_body;

    write(client_socket, http_response.c_str(), http_response.length());
    close(client_socket);
}

// Worker thread run loop
void worker_loop() {
    while (true) {
        int client_socket;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            condition.wait(lock, [] { return !client_queue.empty() || stop_pool; });
            
            if (stop_pool && client_queue.empty()) {
                return;
            }
            
            client_socket = client_queue.front();
            client_queue.pop();
        }
        handle_client(client_socket);
    }
}

int main() {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        std::cerr << "Socket failed" << std::endl;
        return 1;
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        std::cerr << "Setsockopt failed" << std::endl;
        return 1;
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        return 1;
    }

    if (listen(server_fd, 4096) < 0) {
        std::cerr << "Listen failed" << std::endl;
        return 1;
    }

    // Determine hardware concurrency and spin up the pool
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;
    
    std::cout << "C++ Contestant Bot listening on :8080 (Threads: " << num_threads << ")" << std::endl;

    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < num_threads; ++i) {
        workers.emplace_back(worker_loop);
    }

    // Main thread accept loop
    int addrlen = sizeof(address);
    while (true) {
        int new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        if (new_socket >= 0) {
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                client_queue.push(new_socket);
            }
            condition.notify_one();
        }
    }

    // Note: In an actual shutdown scenario, stop_pool would be set to true 
    // and condition.notify_all() called before joining workers.
    return 0;
}