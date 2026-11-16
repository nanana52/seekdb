/*
 * Copyright (c) 2026 OceanBase competition contributors.
 * Licensed under the Apache License, Version 2.0.
 * See the repository LICENSE for the full license text.
 */
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <cuvs/neighbors/hnsw.h>
#include <hip/hip_runtime_api.h>
#include <dlpack/dlpack.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <limits>
#include <set>
#include <vector>
#include <thread>

namespace {
struct BuildResources {
  cuvsResources_t resources{};
  cuvsCagraIndexParams_t graph_params{};
  cuvsCagraIndex_t graph{};
  cuvsHnswIndexParams_t hnsw_params{};
  cuvsHnswIndex_t hnsw{};
  char path[64] = "/tmp/seekdb-hnsw-gpu-XXXXXX";
  bool temporary = false;
  ~BuildResources() {
    if (hnsw) cuvsHnswIndexDestroy(hnsw);
    if (hnsw_params) cuvsHnswIndexParamsDestroy(hnsw_params);
    if (graph) cuvsCagraIndexDestroy(graph);
    if (graph_params) cuvsCagraIndexParamsDestroy(graph_params);
    if (resources) cuvsResourcesDestroy(resources);
    if (temporary) unlink(path);
  }
};
}

// Returns a native HNSW binary, with SQL vector IDs installed in every label.
// The caller owns *data and must call free(). No CPU graph is built here.
static int build_native_hnsw_impl(const float *vectors, const int64_t *ids,
    int64_t count, int dimension, int degree, int ef_construction,
    void **data, uint64_t *bytes)
{
  if (!data || !bytes) return -1;
  *data = nullptr; *bytes = 0;
  if (!vectors || !ids || count < 1 || count > INT32_MAX ||
      dimension < 1 || dimension > 65536 || degree < 4 || degree > 64 ||
      ef_construction < degree || count > INT64_MAX / dimension) return -1;
  try {
    std::set<int64_t> labels;
    for (int64_t i=0; i<count; ++i) {
      if (!labels.insert(ids[i]).second) return -2;
      for (int j=0; j<dimension; ++j)
        if (!std::isfinite(vectors[i*dimension+j])) return -2;
    }
    std::fprintf(stderr,"HIPVS_NATIVE_STAGE resources count=%ld dim=%d\n",static_cast<long>(count),dimension);
    BuildResources state;
    if (cuvsResourcesCreate(&state.resources) != CUVS_SUCCESS ||
        cuvsCagraIndexParamsCreate(&state.graph_params) != CUVS_SUCCESS ||
        cuvsCagraIndexCreate(&state.graph) != CUVS_SUCCESS ||
        cuvsHnswIndexParamsCreate(&state.hnsw_params) != CUVS_SUCCESS ||
        cuvsHnswIndexCreate(&state.hnsw) != CUVS_SUCCESS) return -3;
    std::fprintf(stderr,"HIPVS_NATIVE_STAGE parameters\n");
    state.graph_params->graph_degree = 2*degree;
    state.graph_params->intermediate_graph_degree = 4*degree;
    state.hnsw_params->hierarchy = GPU;
    state.hnsw_params->ef_construction = ef_construction;
    state.hnsw_params->num_threads = 8;
    int64_t shape[2] = {count, dimension};
    DLManagedTensor tensor{};
    tensor.dl_tensor.data = const_cast<float *>(vectors);
    tensor.dl_tensor.device = {kDLCPU,0};
    tensor.dl_tensor.ndim = 2;
    tensor.dl_tensor.dtype = {kDLFloat,32,1};
    tensor.dl_tensor.shape = shape;
    std::fprintf(stderr,"HIPVS_NATIVE_STAGE graph_build\n");
    if (hipDeviceSynchronize() != hipSuccess ||
        cuvsCagraBuild(state.resources,state.graph_params,&tensor,state.graph) != CUVS_SUCCESS ||
        hipDeviceSynchronize() != hipSuccess ||
        cuvsHnswFromCagra(state.resources,state.hnsw_params,state.graph,state.hnsw) != CUVS_SUCCESS ||
        hipDeviceSynchronize() != hipSuccess) return -4;
    std::fprintf(stderr,"HIPVS_NATIVE_STAGE serialize\n");
    int fd=mkstemp(state.path);
    if (fd<0) return -5;
    state.temporary=true;
    close(fd);
    if (cuvsHnswSerialize(state.resources,state.path,state.hnsw) != CUVS_SUCCESS) return -5;
    FILE *file=fopen(state.path,"rb");
    if (!file) return -5;
    struct stat st{};
    if (fstat(fileno(file),&st) != 0 || st.st_size<96 || st.st_size>(2LL<<30)) {
      fclose(file); return -6;
    }
    std::vector<unsigned char> blob(static_cast<size_t>(st.st_size));
    const bool read=fread(blob.data(),1,blob.size(),file)==blob.size();
    fclose(file);
    if (!read) return -6;
    uint64_t actual=0,stride=0,label_offset=0,vector_offset=0;
    std::memcpy(&actual,blob.data()+16,8);
    std::memcpy(&stride,blob.data()+24,8);
    std::memcpy(&label_offset,blob.data()+32,8);
    std::memcpy(&vector_offset,blob.data()+40,8);
    if (actual!=static_cast<uint64_t>(count) || stride<8 ||
        label_offset>stride-8 || vector_offset>stride ||
        static_cast<uint64_t>(dimension)*4>stride-vector_offset ||
        actual>(blob.size()-96)/stride) return -7;
    for (uint64_t i=0; i<actual; ++i) {
      const uint64_t at=96+i*stride+label_offset;
      uint64_t row=0;std::memcpy(&row,blob.data()+at,8);
      if (row>=actual) return -7;
      std::memcpy(blob.data()+at,ids+row,8);
    }
    void *result=std::malloc(blob.size());
    if (!result) return -8;
    std::memcpy(result,blob.data(),blob.size());
    *bytes=blob.size(); *data=result;
    return 0;
  } catch (...) {
    return -9;
  }
}

// Database DDL workers can use small coroutine stacks. ROCm JIT compilation
// requires a regular host thread stack, so all GPU work stays on that thread.
extern "C" int seekdb_cuvs_hnsw_build(const float *vectors, const int64_t *ids,
    int64_t count, int dimension, int degree, int ef_construction,
    void **data, uint64_t *bytes)
{
  if (!data || !bytes) return -1;
  *data=nullptr; *bytes=0;
  int result=-9;
  try {
    std::thread worker([&] {
      result=build_native_hnsw_impl(vectors,ids,count,dimension,degree,ef_construction,data,bytes);
    });
    worker.join();
  } catch (...) {
    std::free(*data);*data=nullptr;*bytes=0;
    return -9;
  }
  return result;
}
