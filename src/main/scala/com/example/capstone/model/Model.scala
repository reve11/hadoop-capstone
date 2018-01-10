package com.example.capstone.model

case class GeoIp(network: String, id: Int, registeredCountryId: Int)
case class GeoData(countryId: Int, localeCode: String, continentCode: String, continentName: String, countryISOCode: String,
                   countryName: String)
case class TopCategory(categoryName: String, purchaseCount: Int)
case class TopCategoryProduct(categoryName: String, productName: String, purchaseCount: Int)
case class TopCountry(countryName: String, totalSpent: BigDecimal)