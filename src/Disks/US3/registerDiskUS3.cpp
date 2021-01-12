#include "Disks/US3/DiskUS3.h"
#include "Disks/DiskFactory.h"
#include "Disks/DiskCacheWrapper.h"
#include "Interpreters/Context.h"
#include "IO/WriteHelpers.h"
#include "IO/ReadHelpers.h"

#include "IO/US3/US3Client.h"

namespace DB {

namespace ErrorCodes {
extern const int BAD_ARGUMENTS;
extern const int PATH_ACCESS_DENIED;
}
namespace {

void check_write_access(IDisk& disk) {
    auto f = disk.writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    f->write("midd", 4);
}

void check_read_access(const String& disk_name, IDisk& disk) {
    auto f = disk.readFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE);
    String buf(4, '0');
    f->readStrict(buf.data(), 4);
    if (buf != "midd") {
        throw Exception("No read access to US3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
    }
}

void check_remove_access(IDisk& disk) {
    disk.remove("test_acl");
}
}

void registerDiskUS3(DiskFactory& factory) {

    auto creator = [](const String& name,
                      const Poco::Util::AbstractConfiguration& config,
                      const String& config_prefix,
                      const Context& context) -> DiskPtr {

        Poco::File disk{context.getPath() + "disks/" + name};
        disk.createDirectories();
        // <disks>
        //     <s3>
        //         <type>s3</type>
        //         <endpoint></endpoint>
        //         <access_key>TOKEN_2afce1b7-abae-49b1-9b4e-4850b230cac0</access_key>
        //         <secret_key>c6fe4245-d86a-4c06-8cd2-6092929515f1</secret_key>
        //         <bucket>testzwb</bucket>
        //     </s3>
        // </disks>

        US3::ClientConfiguration client_config;
        client_config.endpoint_ = config.getString(config_prefix + ".endpoint");
        client_config.access_key_ = config.getString(config_prefix + ".access_key");
        client_config.secret_key_ = config.getString(config_prefix + ".secret_key");
        client_config.bucket_ = config.getString(config_prefix + ".bucket");

        // optional configuration
        client_config.prefix_ = config.getString(config_prefix + ".prefix", "/");
        if (client_config.prefix_.back() != '/') {
            throw Exception("US3 path must ends with '/', but '" + client_config.prefix_ + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);
        }
        auto client = std::make_shared<US3::US3Client>(client_config);
        String local_metadata_path = config.getString(config_prefix + ".metadata_path", context.getPath() + "disks/" + name + "/");

        auto disk_us3 = std::make_shared<DiskUS3>(name, client, client->bucket(), client->prefix(), local_metadata_path,
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
            config.getUInt64(config_prefix + ".max_put_bytes", 64 * 1024 * 1024));

        if (config.getBool(config_prefix + ".access_check", true)) {
            check_write_access(*disk_us3);
            check_read_access(name, *disk_us3);
            check_remove_access(*disk_us3);
        }
        
        if (config.getBool(config_prefix + ".cache_enabled", true)) {
            String cache_path = config.getString(config_prefix + ".cache_path", context.getPath() + "disks/" + name + "/cache/");
            if (local_metadata_path == cache_path) {
                throw Exception("Metadata and cache path should be different:" + local_metadata_path, ErrorCodes::BAD_ARGUMENTS);
            }
            auto cache_disk{std::make_shared<DiskLocal>("us3-cache", cache_path, 0)};
            auto cache_file_pred = [](const String& path) {
                return path.ends_with("idx")
                    || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3")
                    || path.ends_with("txt") || path.ends_with("dat");
            };

            return std::make_shared<DiskCacheWrapper>(disk_us3, cache_disk, cache_file_pred);
        }
        return disk_us3;

    };
    factory.registerDiskType("us3", creator);
}

}