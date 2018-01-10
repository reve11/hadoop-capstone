package com.example.capstone.service

import com.example.capstone.model.{TopCategory, TopCategoryProduct, TopCountry}

trait EventAggregator extends Serializable {

  type AggregationResult = (Iterable[TopCategory], Iterable[TopCategoryProduct], Iterable[TopCountry])

  def aggregateEvents(): AggregationResult
}
