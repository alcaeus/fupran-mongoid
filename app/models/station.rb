class Station
  include Mongoid::Document

  field :_id, type: String
  field :name, type: String
  field :brand, type: String
  field :street, type: String
  field :house_number, type: Integer
  field :post_code, type: Integer
  field :place, type: String
  field :latitude, type: Float
  field :longitude, type: Float

  has_many :price_reports, inverse_of: :station, order: :report_time.desc do
    def latest_price
      first
    end
  end

  def changes_per_day
    result = PriceReport.collection.aggregate(
      [
        match_station,
        *create_daily_stats
      ]
    ).to_a[0]

    result == nil ? 0 : result['changesPerDay']
  end

  def price_differences
    PriceReport.collection.aggregate(
      [
        *get_diff_pipeline,
        sort_by_timeframe
      ]
    )
  end

  def aggregated_prices_pipeline
    [
      match_station,
      create_timeframe_stats,
      add_average_change,
      shape_output_document,
    ]
  end

  def aggregated_prices
    PriceReport.collection.aggregate(
      [
        *aggregated_prices_pipeline,
        sort_by_timeframe,
      ]
    ).to_a
  end

  def popular_increase_times
    PriceReport.collection.aggregate(
      get_change_count_pipeline(match_price_increases)
    )
  end

  def popular_decrease_times
    PriceReport.collection.aggregate(
      get_change_count_pipeline(match_price_decreases)
    )
  end

  def popular_cheapest_times
    PriceReport.collection.aggregate(get_popular_extreme_times_pipeline('lowest'))
  end

  def popular_expensive_times
    PriceReport.collection.aggregate(get_popular_extreme_times_pipeline('highest'))
  end

  private
    def match_station
      {
        '$match' => {
          'station_id' => self._id,
          'previous_price_report' => {
            '$ne' => nil
          }
        }
      }
    end

    def create_timeframe_stats
      {
        '$group' => {
          '_id' => '$report_timeframe',
          'changes' => {
            '$sum' => 1
          },
          'dieselChange' => {
            '$avg' => '$diesel.change'
          },
          'e5Change' => {
            '$avg' => '$e5.change'
          },
          'e10Change' => {
            '$avg' => '$e10.change'
          }
        }
      }
    end

    def create_daily_stats
      [
        {
          '$group' => {
            '_id' => {
              'year' => { '$year' => '$report_time' },
              'month' => { '$month' => '$report_time' },
              'day' => { '$dayOfMonth' => '$report_time' }
            },
            'changes' => { '$sum' => 1 }
          }
        },
        {
          '$group' => {
            '_id' => nil,
            'changesPerDay' => { '$avg' => '$changes' }
          }
        }
      ]
    end

    def add_average_change
      {
        '$addFields' => {
          'averageChange' => {
            '$avg' => ['$dieselChange', '$e5Change', '$e10Change']
          }
        }
      }
    end

    def match_price_increases
      {
        '$match' => {
          'averageChange' => { '$gt' => 0 }
        }
      }
    end

    def match_price_decreases
      {
        '$match' => {
          'averageChange' => { '$lt' => 0 }
        }
      }
    end

    def sort_by_timeframe
      {
        '$sort' => {
          'timeframe.hour' => 1,
          'timeframe.minute' => 1
        }
      }
    end

    def sort_by_changes
      {
        '$sort' => {
          'changes' => -1,
          'timeframe.hour' => 1,
          'timeframe.minute' => 1,
        }
      }
    end

    def shape_output_document
      {
        '$addFields' => {
          'timeframe' => '$_id'
        }
      }
    end

    def get_change_count_pipeline(matcher)
      [
        *aggregated_prices_pipeline,
        matcher,
        sort_by_changes,
        {
          '$limit' => 5,
        }
      ]
    end

    def group_data_by_day
      {
        '$group' => {
          '_id' => {
            'year' => {
              '$year' => '$report_time'
            },
            'month' => {
              '$month' => '$report_time'
            },
            'day' => {
              '$dayOfMonth' => '$report_time'
            }
          },
          'changes' => {
            '$sum' => 1
          },
          'dieselData' => {
            '$push' => {
              'report_timeframe' => '$report_timeframe',
              'price' => '$diesel.price',
              'change' => '$diesel.change'
            }
          },
          'e5Data' => {
            '$push' => {
              'report_timeframe' => '$report_timeframe',
              'price' => '$e5.price',
              'change' => '$e5.change'
            }
          },
          'e10Data' => {
            '$push' => {
              'report_timeframe' => '$report_timeframe',
              'price' => '$e10.price',
              'change' => '$e10.change'
            }
          }
        }
      }
    end

    def get_fuel_difference_pipeline(fuel_type)
      [
        {
          '$set' => {
            data_field_name(fuel_type) => {
              '$sortArray' => {
                'input' => '$' + data_field_name(fuel_type),
                'sortBy' => {
                  'price' => 1,
                  'report_timeframe.hour' => 1,
                  'report_timeframe.minute' => 1
                }
              }
            }
          }
        },
        {
          '$addFields' => {
            fuel_type => {
              'lowest' => {
                '$first' => '$' + data_field_name(fuel_type)
              },
              'highest' => {
                '$last' => '$' + data_field_name(fuel_type)
              }
            }
          }
        },
        {
          '$addFields' => {
            data_field_name(fuel_type) => {
              '$sortArray' => {
                'input' => {
                  '$map' => {
                    'input' => '$' + data_field_name(fuel_type),
                    'as' => 'price_report',
                    'in' => {
                      'report_timeframe' => '$$price_report.report_timeframe',
                      'higher_than_lowest' => {
                        '$subtract' => [
                          '$$price_report.price', '$$ROOT.' + fuel_type + '.lowest.price'
                        ]
                      }
                    }
                  }
                },
                'sortBy' => {
                  'report_timeframe.hour' => 1,
                  'report_timeframe.minute' => 1
                }
              }
            }
          }
        },
        {
          '$unwind' => {
            'path' => '$' + data_field_name(fuel_type)
          }
        },
        {
          '$group' => {
            '_id' => '$' + data_field_name(fuel_type) + '.report_timeframe',
            'higherThanLowest' => {
              '$avg' => '$' + data_field_name(fuel_type) + '.higher_than_lowest'
            },
            'times' => {
              '$sum' => 1
            }
          }
        },
        {
          '$replaceWith' => {
            '_id' => '$_id',
            fuel_type => {
              'higherThanLowest' => '$higherThanLowest',
              'times' => '$times',
              'impact' => { '$multiply' => [ '$higherThanLowest', '$times' ] }
            }
          }
        }
      ]
    end

    def get_diff_pipeline
      [
        match_station,
        group_data_by_day,
        {
          '$facet' => {
            'diesel' => get_fuel_difference_pipeline('diesel'),
            'e5' => get_fuel_difference_pipeline('e5'),
            'e10' => get_fuel_difference_pipeline('e10'),
          }
        },
        {
          '$replaceRoot' => {
            'newRoot' => {
              'data' => {
                '$concatArrays' => [
                  '$diesel', '$e5', '$e10'
                ]
              }
            }
          }
        },
        {
          '$unwind' => {
            'path' => '$data'
          }
        },
        {
          '$group' => {
            '_id' => '$data._id',
            'diesel' => {
              '$push' => '$data.diesel'
            },
            'e5' => {
              '$push' => '$data.e5'
            },
            'e10' => {
              '$push' => '$data.e10'
            }
          }
        },
        {
          '$addFields' => {
            'diesel' => {
              '$first' => '$diesel'
            },
            'e5' => {
              '$first' => '$e5'
            },
            'e10' => {
              '$first' => '$e10'
            }
          }
        },
        {
          '$addFields' => {
            'average' => {
              'higherThanLowest' => {
                '$avg' => [
                  '$diesel.higherThanLowest', '$e5.higherThanLowest', '$e10.higherThanLowest'
                ]
              },
              'times' => {
                '$avg' => [
                  '$diesel.times', '$e5.times', '$e10.times'
                ]
              }
            }
          }
        },
        {
          '$addFields' => {
            'average' => {
              'impact' => {
                '$multiply' => [
                  '$average.higherThanLowest', '$average.times'
                ]
              }
            }
          }
        },
        shape_output_document
      ]
    end

    def sort_fuel_data_by_price(fuel_type)
      {
        '$sortArray' => {
          'input' => '$' + data_field_name(fuel_type),
          'sortBy' => {
            'price' => 1,
            'report_timeframe.hour' => 1,
            'report_timeframe.minute' => 1
          }
        }
      }
    end

    def find_extreme_price_for_fuel(fuel_type)
      {
        'lowestEntry' => {
          '$first' => '$' + data_field_name(fuel_type)
        },
        'highestEntry' => {
          '$last' => '$' + data_field_name(fuel_type)
        }
      }
    end

    def find_extreme_times_for_fuel(fuel_type)
      {
        'lowestTimes' => {
          '$filter' => {
            'input' => '$' + data_field_name(fuel_type),
            'as' => 'priceReport',
            'cond' => {
              '$eq' => [
                '$$priceReport.price', '$' + fuel_type + '.lowestEntry.price'
              ]
            }
          }
        },
        'highestTimes' => {
          '$filter' => {
            'input' => '$' + data_field_name(fuel_type),
            'as' => 'priceReport',
            'cond' => {
              '$eq' => [
                '$$priceReport.price', '$' + fuel_type + '.highestEntry.price'
              ]
            }
          }
        }
      }
    end

    def group_extreme_times_for_fuel(fuel_type, type, limit)
      [
        {
          '$unwind' => {
            'path' => '$' + fuel_type + '.' + type + 'Times'
          }
        },
        {
          '$group' => {
            '_id' => '$' + fuel_type + '.' + type + 'Times.report_timeframe',
            'count' => {
              '$sum' => 1
            }
          }
        },
        {
          '$sort' => {
            'count' => -1
          }
        },
        {
          '$limit' => limit
        }
      ]
    end

    def shape_extreme_times_for_fuel(fuel_type)
      {
        '$map' => {
          'input' => '$' + fuel_type,
          'as' => 'report',
          'in' => {
            fuel_type => {
              'timeframe' => '$$report._id',
              'count' => '$$report.count'
            }
          }
        }
      }
    end

    def shape_extreme_times(limit)
      elements = []

      for i in 0..limit-1 do
        elements.push(
          {
            '$mergeObjects' => [
              { '$arrayElemAt' => ['$diesel', i] },
              { '$arrayElemAt' => ['$e5', i] },
              { '$arrayElemAt' => ['$e10', i] },
            ],
          }
        )
      end

      {
        data: elements
      }
    end

    def get_popular_extreme_times_pipeline(type)
      limit = 3

      [
        match_station,
        group_data_by_day,
        {
          '$set' => {
            'dieselData' => sort_fuel_data_by_price('diesel'),
            'e5Data' => sort_fuel_data_by_price('e5'),
            'e10Data' => sort_fuel_data_by_price('e10'),
          }
        },
        {
          '$addFields' => {
            'diesel' => find_extreme_price_for_fuel('diesel'),
            'e5' => find_extreme_price_for_fuel('e5'),
            'e10' => find_extreme_price_for_fuel('e10'),
          }
        },
        {
          '$addFields' => {
            'diesel' => find_extreme_times_for_fuel('diesel'),
            'e5' => find_extreme_times_for_fuel('e5'),
            'e10' => find_extreme_times_for_fuel('e10'),
          }
        },
        {
          '$facet' => {
            'diesel' => group_extreme_times_for_fuel('diesel', type, limit),
            'e5' => group_extreme_times_for_fuel('e5', type, limit),
            'e10' => group_extreme_times_for_fuel('e10', type, limit),
          }
        },
        {
          '$replaceWith' => {
            'diesel' => shape_extreme_times_for_fuel('diesel'),
            'e5' => shape_extreme_times_for_fuel('e5'),
            'e10' => shape_extreme_times_for_fuel('e10'),
          }
        },
        {
          '$replaceWith' => shape_extreme_times(limit)
        },
        {
          '$unwind': '$data'
        },
        {
          '$replaceRoot' => { 'newRoot' => '$data' }
        }
      ]
    end

    def data_field_name(fuel_type)
      fuel_type + 'Data'
    end
end
