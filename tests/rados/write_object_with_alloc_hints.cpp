#include <rados/librados.hpp>
#include <iostream>
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <cstdlib>

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <pool_name> <num_objects> <write_size_in_bytes> <alloc_hint>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string pool_name = argv[1];
    int num_objects = std::atoi(argv[2]);
    int user_provided_alloc_hint = std::atoi(argv[4]);
    uint64_t write_size = std::strtoull(argv[3], nullptr, 10);
    uint64_t object_size = 4 * 1024 * 1024; // Expected object size for allocation hint

    if (pool_name.empty() || num_objects <= 0 || write_size == 0) {
        std::cerr << "Invalid input. Ensure pool name is not empty, num_objects > 0, and write_size > 0." << std::endl;
        return EXIT_FAILURE;
    }

    // Create data to write
    std::string data(write_size, 'X');

    librados::Rados cluster;
    int ret = cluster.init("admin");
    if (ret < 0) {
        std::cerr << "Failed to initialize cluster handle: " << ret << std::endl;
        return EXIT_FAILURE;
    }

    ret = cluster.conf_read_file("/etc/ceph/ceph.conf");
    if (ret < 0) {
        std::cerr << "Failed to read Ceph config: " << ret << std::endl;
        return EXIT_FAILURE;
    }

    ret = cluster.connect();
    if (ret < 0) {
        std::cerr << "Failed to connect to cluster: " << ret << std::endl;
        return EXIT_FAILURE;
    }

    librados::IoCtx ioctx;
    ret = cluster.ioctx_create(pool_name.c_str(), ioctx);
    if (ret < 0) {
        std::cerr << "Failed to open pool '" << pool_name << "': " << ret << std::endl;
        return EXIT_FAILURE;
    }

    for (int i = 0; i < num_objects; ++i) {
        auto now = std::chrono::system_clock::now();
        std::time_t t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << "myobject_" << std::put_time(std::localtime(&t), "%Y%m%d_%H%M%S") << "_" << i;
        std::string object_name = ss.str();

        ret = ioctx.set_alloc_hint2(object_name, object_size, write_size, user_provided_alloc_hint);
        if (ret < 0) {
            std::cerr << "Failed to set allocation hint for object '" << object_name << "': " << ret << std::endl;
            continue;
        }

        librados::bufferlist bl;
        bl.append(data);
        ret = ioctx.write_full(object_name, bl);
        if (ret < 0) {
            std::cerr << "Failed to write object '" << object_name << "': " << ret << std::endl;
        } else {
            std::cout << "Wrote object '" << object_name << "' (" << write_size << " bytes) to pool '" << pool_name << "'." << std::endl;
        }
    }

    ioctx.close();
    cluster.shutdown();
    return EXIT_SUCCESS;
}