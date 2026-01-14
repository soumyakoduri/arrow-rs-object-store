// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/**
 * C wrapper for RGW SAL (Storage Abstraction Layer) APIs
 *
 * This file provides C-compatible wrapper functions around the C++ RGW SAL APIs,
 * allowing Rust code to interact with SAL via FFI.
 *
 * Build requirements:
 * - Ceph development headers (librados-dev, librgw-dev)
 * - C++17 compiler
 * - Link against: -lrados -lrgw
 */

#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <map>

// RGW SAL headers
#include "rgw/rgw_sal.h"
#include "rgw/rgw_sal_rados.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"

// C linkage for FFI
extern "C" {

// Forward declarations of opaque types
struct SalDriver;
struct SalBucket;
struct SalObject;
struct SalWriter;
struct SalMultipartUpload;

// Object metadata structure
struct SalObjectMeta {
    uint64_t size;
    int64_t mtime;
    const char* etag;
};

// === Initialization Functions ===

/**
 * Create a CephContext for SAL operations
 */
CephContext* sal_create_ceph_context(
    const char* cluster_name,
    const char* user_name,
    const char* conf_file)
{
    try {
        // Create CephContext
        std::vector<const char*> args;

        if (conf_file != nullptr) {
            args.push_back("--conf");
            args.push_back(conf_file);
        }

        args.push_back("--name");
        std::string user_str = std::string("client.") + user_name;
        args.push_back(user_str.c_str());

        CephContext* cct = common_preinit(
            static_cast<CephInitParameters>(CEPH_ENTITY_TYPE_CLIENT),
            static_cast<int>(args.size()),
            const_cast<char**>(args.data()),
            CINIT_FLAG_NO_DEFAULT_CONFIG_FILE
        );

        if (!cct) {
            return nullptr;
        }

        // Read config file if provided
        if (conf_file != nullptr) {
            cct->_conf.parse_config_files(conf_file, nullptr, 0);
        }

        cct->_conf.apply_changes(nullptr);

        return cct;
    } catch (...) {
        return nullptr;
    }
}

/**
 * Configure authentication for CephContext
 */
int sal_configure_auth(
    CephContext* cct,
    const char* keyring,
    const char* key)
{
    if (!cct) {
        return -EINVAL;
    }

    try {
        if (keyring != nullptr) {
            cct->_conf.set_val("keyring", keyring);
        }

        if (key != nullptr) {
            cct->_conf.set_val("key", key);
        }

        cct->_conf.apply_changes(nullptr);
        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Create a SAL Driver (RADOSStore backend)
 */
rgw::sal::Driver* sal_create_rados_driver(
    CephContext* cct,
    DoutPrefixProvider* dpp)
{
    if (!cct) {
        return nullptr;
    }

    try {
        // Create RADOSStore driver
        rgw::sal::Driver* driver = rgw::sal::StoreManager::get_storage(
            dpp,
            cct,
            "rados",  // backend type
            false,    // use data pool
            false     // run sync thread
        );

        return driver;
    } catch (...) {
        return nullptr;
    }
}

/**
 * Destroy SAL Driver
 */
void sal_destroy_driver(rgw::sal::Driver* driver)
{
    delete driver;
}

/**
 * Destroy CephContext
 */
void sal_destroy_ceph_context(CephContext* cct)
{
    if (cct) {
        cct->put();
    }
}

// === Bucket Operations ===

/**
 * Get a bucket by name
 */
rgw::sal::Bucket* sal_get_bucket(
    rgw::sal::Driver* driver,
    DoutPrefixProvider* dpp,
    const char* bucket_name,
    const char* tenant)
{
    if (!driver || !bucket_name) {
        return nullptr;
    }

    try {
        RGWBucketInfo info;
        info.bucket.name = bucket_name;

        if (tenant != nullptr) {
            info.bucket.tenant = tenant;
        }

        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = driver->get_bucket(dpp, nullptr, info, &bucket);

        if (ret < 0 || !bucket) {
            return nullptr;
        }

        return bucket.release();
    } catch (...) {
        return nullptr;
    }
}

/**
 * Destroy SAL Bucket
 */
void sal_destroy_bucket(rgw::sal::Bucket* bucket)
{
    delete bucket;
}

/**
 * List objects in a bucket
 */
int sal_list_objects(
    rgw::sal::Bucket* bucket,
    DoutPrefixProvider* dpp,
    const char* prefix,
    const char* delimiter,
    int max_keys,
    const char* marker,
    char*** out_keys,
    int* out_count)
{
    if (!bucket || !out_keys || !out_count) {
        return -EINVAL;
    }

    try {
        rgw::sal::Bucket::ListParams params;
        rgw::sal::Bucket::ListResults results;

        if (prefix != nullptr) {
            params.prefix = prefix;
        }

        if (delimiter != nullptr) {
            params.delim = delimiter;
        }

        if (marker != nullptr) {
            params.marker = rgw_obj_key(marker);
        }

        params.list_versions = false;
        params.allow_unordered = false;

        int ret = bucket->list(dpp, params, max_keys, results, null_yield);

        if (ret < 0) {
            return ret;
        }

        // Allocate array of C strings
        size_t count = results.objs.size();
        char** keys = new char*[count];

        for (size_t i = 0; i < count; ++i) {
            const std::string& key = results.objs[i].key.name;
            keys[i] = new char[key.length() + 1];
            std::strcpy(keys[i], key.c_str());
        }

        *out_keys = keys;
        *out_count = static_cast<int>(count);

        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Free object list (matching the FFI interface name)
 */
void sal_free_list_result(char** keys, int count)
{
    if (!keys) {
        return;
    }

    for (int i = 0; i < count; ++i) {
        delete[] keys[i];
    }
    delete[] keys;
}

// === Object Operations ===

/**
 * Get an object from a bucket
 */
rgw::sal::Object* sal_get_object(
    rgw::sal::Bucket* bucket,
    DoutPrefixProvider* dpp,
    const char* key)
{
    if (!bucket || !key) {
        return nullptr;
    }

    try {
        rgw_obj_key obj_key(key);
        std::unique_ptr<rgw::sal::Object> object = bucket->get_object(obj_key);

        if (!object) {
            return nullptr;
        }

        return object.release();
    } catch (...) {
        return nullptr;
    }
}

/**
 * Destroy SAL Object
 */
void sal_destroy_object(rgw::sal::Object* object)
{
    delete object;
}

/**
 * Get object metadata
 */
int sal_get_object_meta(
    rgw::sal::Object* object,
    DoutPrefixProvider* dpp,
    SalObjectMeta* out_meta)
{
    if (!object || !out_meta) {
        return -EINVAL;
    }

    try {
        rgw::sal::Attrs attrs;
        int ret = object->get_obj_attrs(dpp, null_yield, nullptr);

        if (ret < 0) {
            return ret;
        }

        // Get object state to retrieve metadata
        RGWObjState* state = nullptr;
        ret = object->get_obj_state(dpp, &state, null_yield);

        if (ret < 0 || !state) {
            return ret;
        }

        out_meta->size = state->size;
        out_meta->mtime = state->mtime.sec();
        out_meta->etag = state->etag.empty() ? nullptr : state->etag.c_str();

        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Read object data
 */
int sal_read_object(
    rgw::sal::Object* object,
    DoutPrefixProvider* dpp,
    uint64_t offset,
    uint64_t length,
    char** out_buffer,
    uint64_t* out_bytes_read)
{
    if (!object || !out_buffer || !out_bytes_read) {
        return -EINVAL;
    }

    try {
        // Create read operation
        std::unique_ptr<rgw::sal::Object::ReadOp> read_op = object->get_read_op();

        if (!read_op) {
            return -EIO;
        }

        // Prepare read
        int ret = read_op->prepare(dpp, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Read data
        bufferlist bl;
        ret = read_op->read(offset, length, bl, dpp, null_yield);

        if (ret < 0) {
            return ret;
        }

        // Copy data to C buffer
        *out_bytes_read = bl.length();
        *out_buffer = new char[bl.length()];
        bl.begin().copy(bl.length(), *out_buffer);

        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Free read buffer
 */
void sal_free_read_buffer(char* buffer)
{
    delete[] buffer;
}

/**
 * Delete an object
 */
int sal_delete_object(
    rgw::sal::Object* object,
    DoutPrefixProvider* dpp)
{
    if (!object) {
        return -EINVAL;
    }

    try {
        // Create delete operation
        std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = object->get_delete_op();

        if (!del_op) {
            return -EIO;
        }

        // Perform delete
        int ret = del_op->delete_obj(dpp, null_yield);

        return ret;
    } catch (...) {
        return -EIO;
    }
}

// === Writer Operations ===

/**
 * Create a writer for putting an object
 */
rgw::sal::Writer* sal_create_writer(
    rgw::sal::Driver* driver,
    DoutPrefixProvider* dpp,
    rgw::sal::Bucket* bucket,
    const char* key)
{
    if (!driver || !bucket || !key) {
        return nullptr;
    }

    try {
        rgw_obj_key obj_key(key);
        std::unique_ptr<rgw::sal::Object> object = bucket->get_object(obj_key);

        if (!object) {
            return nullptr;
        }

        std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
            dpp,
            null_yield,
            object.get(),
            bucket->get_owner(),
            nullptr,  // obj_ctx
            nullptr   // olh_epoch
        );

        return writer.release();
    } catch (...) {
        return nullptr;
    }
}

/**
 * Destroy SAL Writer
 */
void sal_destroy_writer(rgw::sal::Writer* writer)
{
    delete writer;
}

/**
 * Prepare writer
 */
int sal_writer_prepare(
    rgw::sal::Writer* writer,
    DoutPrefixProvider* dpp)
{
    if (!writer) {
        return -EINVAL;
    }

    try {
        return writer->prepare(dpp, null_yield);
    } catch (...) {
        return -EIO;
    }
}

/**
 * Write data
 */
int sal_writer_write(
    rgw::sal::Writer* writer,
    DoutPrefixProvider* dpp,
    const char* data,
    uint64_t length,
    uint64_t offset)
{
    if (!writer || !data) {
        return -EINVAL;
    }

    try {
        bufferlist bl;
        bl.append(data, length);

        return writer->process(std::move(bl), offset);
    } catch (...) {
        return -EIO;
    }
}

/**
 * Complete write
 */
int sal_writer_complete(
    rgw::sal::Writer* writer,
    DoutPrefixProvider* dpp,
    char** out_etag)
{
    if (!writer || !out_etag) {
        return -EINVAL;
    }

    try {
        std::string etag;
        ceph::real_time mtime;

        int ret = writer->complete(
            0,        // accounted_size
            etag,
            &mtime,
            ceph::real_time(),  // set_mtime
            nullptr,            // attrs
            ceph::real_time(),  // delete_at
            nullptr,            // if_match
            nullptr,            // if_nomatch
            nullptr,            // user_data
            nullptr,            // zones_trace
            nullptr             // canceled
        );

        if (ret < 0) {
            return ret;
        }

        // Copy ETag to output
        *out_etag = new char[etag.length() + 1];
        std::strcpy(*out_etag, etag.c_str());

        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Free ETag string
 */
void sal_free_etag(char* etag)
{
    delete[] etag;
}

// === Multipart Upload Operations ===

/**
 * Initiate a multipart upload
 */
rgw::sal::MultipartUpload* sal_init_multipart(
    rgw::sal::Driver* driver,
    DoutPrefixProvider* dpp,
    rgw::sal::Bucket* bucket,
    const char* key,
    char** out_upload_id)
{
    if (!driver || !bucket || !key || !out_upload_id) {
        return nullptr;
    }

    try {
        rgw_obj_key obj_key(key);
        std::unique_ptr<rgw::sal::Object> object = bucket->get_object(obj_key);

        if (!object) {
            return nullptr;
        }

        // Get multipart upload object
        std::unique_ptr<rgw::sal::MultipartUpload> upload =
            driver->get_multipart_upload(object.get());

        if (!upload) {
            return nullptr;
        }

        // Initialize the upload
        ACLOwner owner = bucket->get_owner();
        rgw_placement_rule dest_placement;

        int ret = upload->init(dpp, null_yield, owner, dest_placement, {});

        if (ret < 0) {
            return nullptr;
        }

        // Get and copy upload ID
        std::string upload_id = upload->get_upload_id();
        *out_upload_id = new char[upload_id.length() + 1];
        std::strcpy(*out_upload_id, upload_id.c_str());

        return upload.release();
    } catch (...) {
        return nullptr;
    }
}

/**
 * Destroy multipart upload
 */
void sal_destroy_multipart(rgw::sal::MultipartUpload* upload)
{
    delete upload;
}

/**
 * Upload a part
 */
int sal_upload_part(
    rgw::sal::MultipartUpload* upload,
    DoutPrefixProvider* dpp,
    int part_num,
    const char* data,
    uint64_t length,
    char** out_etag)
{
    if (!upload || !data || !out_etag) {
        return -EINVAL;
    }

    try {
        bufferlist bl;
        bl.append(data, length);

        std::string etag;
        int ret = upload->upload_part(
            dpp,
            null_yield,
            part_num,
            bl,
            0,           // offset
            length,      // size
            &etag
        );

        if (ret < 0) {
            return ret;
        }

        // Copy ETag to output
        *out_etag = new char[etag.length() + 1];
        std::strcpy(*out_etag, etag.c_str());

        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Complete multipart upload
 */
int sal_complete_multipart(
    rgw::sal::MultipartUpload* upload,
    DoutPrefixProvider* dpp,
    const char** part_etags,
    int num_parts,
    char** out_final_etag)
{
    if (!upload || !part_etags || !out_final_etag) {
        return -EINVAL;
    }

    try {
        // Build part ETags map
        std::map<int, std::string> parts;
        for (int i = 0; i < num_parts; ++i) {
            parts[i + 1] = part_etags[i];
        }

        ceph::real_time mtime;
        std::string final_etag;

        int ret = upload->complete(
            dpp,
            null_yield,
            mtime,
            {},          // remove_objs
            parts,
            {},          // optional_params
            &final_etag
        );

        if (ret < 0) {
            return ret;
        }

        // Copy final ETag to output
        *out_final_etag = new char[final_etag.length() + 1];
        std::strcpy(*out_final_etag, final_etag.c_str());

        return 0;
    } catch (...) {
        return -EIO;
    }
}

/**
 * Abort multipart upload
 */
int sal_abort_multipart(
    rgw::sal::MultipartUpload* upload,
    DoutPrefixProvider* dpp)
{
    if (!upload) {
        return -EINVAL;
    }

    try {
        return upload->abort(dpp, null_yield);
    } catch (...) {
        return -EIO;
    }
}

/**
 * Free upload ID string
 */
void sal_free_upload_id(char* upload_id)
{
    delete[] upload_id;
}

// === Copy Operations ===

/**
 * Copy an object (server-side)
 */
int sal_copy_object(
    rgw::sal::Driver* driver,
    DoutPrefixProvider* dpp,
    rgw::sal::Bucket* src_bucket,
    const char* src_key,
    rgw::sal::Bucket* dst_bucket,
    const char* dst_key)
{
    if (!driver || !src_bucket || !src_key || !dst_bucket || !dst_key) {
        return -EINVAL;
    }

    try {
        // Get source object
        rgw_obj_key src_obj_key(src_key);
        std::unique_ptr<rgw::sal::Object> src_object = src_bucket->get_object(src_obj_key);

        if (!src_object) {
            return -ENOENT;
        }

        // Get destination object
        rgw_obj_key dst_obj_key(dst_key);
        std::unique_ptr<rgw::sal::Object> dst_object = dst_bucket->get_object(dst_obj_key);

        if (!dst_object) {
            return -EINVAL;
        }

        // Perform copy
        int ret = src_object->copy_object(
            dpp,
            nullptr,      // user
            dst_bucket,
            dst_object.get(),
            null_yield
        );

        return ret;
    } catch (...) {
        return -EIO;
    }
}

} // extern "C"
