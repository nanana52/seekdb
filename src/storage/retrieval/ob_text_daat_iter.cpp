/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_text_daat_iter.h"

namespace oceanbase
{
namespace storage
{
int ObTextDaaTIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_,
                                            *param.allocator_, *param.relevance_collector_))) {
  } else if (param.base_param_->relevance_expr_ != nullptr &&
             OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  } else {
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
    output_required_ = param.output_required_;
    minimum_should_match_ = param.minimum_should_match_;
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  bm25_param_estimator_.reset();
  // this class can know the type of dim_iters_, but ObSRDaaTIterImpl maybe not
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRDaaTIterImpl::reset();
}

void ObTextDaaTIter::reuse(const bool switch_tablet)
{
  if ((OB_NOT_NULL(dim_iters_) && 0 == dim_iters_->count())) {
    // do nothing
  } else {
    bm25_param_estimator_.reuse(switch_tablet);
  }
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRDaaTIterImpl::reuse(switch_tablet);
}

int ObTextDaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (iter_param_->relevance_expr_ != nullptr) {
    if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
    }
  }
  return ret;
}

int ObTextDaaTIter::fill_merge_heap()
{
  if (output_required_ || dim_iters_->count() > ObSRSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    return ObSRDaaTIterImpl::fill_merge_heap();
  }
  int ret = OB_SUCCESS;
  ObSRSimpleMerger *heap = static_cast<ObSRSimpleMerger *>(merge_heap_);
  ObISRDaaTDimIter *const *dims = dim_iters_->get_data();
  const int64_t *next = next_round_iter_idxes_.get_data();
  const ObDatum **ids = iter_domain_ids_.get_data();
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t idx = next[i];
    ObTextRetrievalDaaTTokenIter *dim = static_cast<ObTextRetrievalDaaTTokenIter *>(dims[idx]);
    ObSRMergeItem item;
    if (OB_ISNULL(dim)) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(dim->ObTextRetrievalDaaTTokenIter::get_next_row())) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    } else if (OB_FAIL(dim->ObTextRetrievalDaaTTokenIter::get_curr_id(ids[idx]))) {
    } else {
      item.iter_idx_ = idx;
      // This plan returns cardinality only. Scores and boost products have
      // no consumer; minimum matched dimensions are checked independently.
      item.relevance_ = 0.0;
      ret = heap->ObSRSimpleMerger::push(item);
    }
  }
  if (OB_SUCC(ret)) {
    if (heap->ObSRSimpleMerger::empty()) { ret = OB_ITER_END; }
    else { next_round_cnt_ = 0; }
  }
  return ret;
}

int ObTextDaaTIter::collect_dims_by_id(const ObDatum *&id_datum,
    double &relevance, bool &got_valid_id)
{
  if (output_required_) {
    return ObSRDaaTIterImpl::collect_dims_by_id(id_datum, relevance, got_valid_id);
  }
  int ret = OB_SUCCESS;
  int64_t matched_count = 0;
  int64_t last_iter = 0;
  bool document_complete = false;
  const ObSRMergeItem *item = nullptr;
  if (dim_iters_->count() <= ObSRSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    ObSRSimpleMerger *heap = static_cast<ObSRSimpleMerger *>(merge_heap_);
    while (OB_SUCC(ret) && !heap->ObSRSimpleMerger::empty() && !document_complete) {
      document_complete = heap->ObSRSimpleMerger::is_unique_champion();
      if (OB_FAIL(heap->ObSRSimpleMerger::top(item))) {
      } else {
        last_iter = item->iter_idx_;
        if (OB_FAIL(heap->ObSRSimpleMerger::pop())) {
        } else {
          next_round_iter_idxes_[next_round_cnt_++] = last_iter;
          ++matched_count;
        }
      }
    }
  } else {
    while (OB_SUCC(ret) && !merge_heap_->empty() && !document_complete) {
      document_complete = merge_heap_->is_unique_champion();
      if (OB_FAIL(merge_heap_->top(item))) {
      } else {
        last_iter = item->iter_idx_;
        if (OB_FAIL(merge_heap_->pop())) {
        } else {
          next_round_iter_idxes_[next_round_cnt_++] = last_iter;
          ++matched_count;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    id_datum = iter_domain_ids_[last_iter];
    if (OB_ISNULL(id_datum)) { ret = OB_ERR_UNEXPECTED; }
    else {
      // No relevance value can escape this plan. Preserve the same minimum
      // matched-dimension predicate without computing an unused inner product.
      relevance = 0.0;
      got_valid_id = matched_count >= minimum_should_match_;
    }
  }
  return ret;
}

int ObTextDaaTIter::cache_result(int64_t &count, const ObDatum &id_datum, const double relevance)
{
  if (output_required_) {
    return ObSRDaaTIterImpl::cache_result(count, id_datum, relevance);
  }
  // Only row cardinality is consumed above this iterator. The normal merge
  // has already performed all reads, duplicate elimination and predicates.
  const common::ObLimitParam *limit = iter_param_->limit_param_;
  ++input_row_cnt_;
  if (limit->is_valid() && input_row_cnt_ <= limit->offset_) { return OB_SUCCESS; }
  ++count;
  ++output_row_cnt_;
  return limit->is_valid() && output_row_cnt_ >= limit->limit_ ? OB_ITER_END : OB_SUCCESS;
}

int ObTextDaaTIter::project_results(const int64_t count)
{
  return output_required_ ? ObSRDaaTIterImpl::project_results(count) : OB_SUCCESS;
}

ObTextBMWIter::ObTextBMWIter()
  : ObSRBMWIterImpl(),
    bm25_param_estimator_() {}

void ObTextBMWIter::reuse(const bool switch_tablet)
{
  bm25_param_estimator_.reuse(switch_tablet);
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRBMWIterImpl::reuse(switch_tablet);
}

void ObTextBMWIter::reset()
{
  bm25_param_estimator_.reset();
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRBMWIterImpl::reset();
}

int ObTextBMWIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_,
                                           *param.allocator_, *param.relevance_collector_))) {
  } else if (OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  }
  return ret;
}

int ObTextBMWIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
  }

  if (FAILEDx(ObSRBMWIterImpl::get_next_rows(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    }
  }
  return ret;
}

int ObTextBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bm25_param_estimator_.is_estimated())) {
    ret = OB_ERR_UNEXPECTED;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObTextRetrievalBlockMaxIter *block_max_iter = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter(
        bm25_param_estimator_.get_total_doc_cnt(), bm25_param_estimator_.get_avg_doc_token_cnt()))) {
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
