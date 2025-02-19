require 'pipeline_stage'
require 'pipeline_operator'

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
      PipelineStage.match(
        station_id: self._id,
        previous_price_report: { '$ne' => nil })
    end

    def create_timeframe_stats
      PipelineStage.group(
        '$report_timeframe',
        changes: PipelineOperator.sum(1),
        dieselChange: PipelineOperator.avg('$diesel.change'),
        e5Change: PipelineOperator.avg('$e5.change'),
        e10Change: PipelineOperator.avg('$e10.change'),
      )
    end

    def create_daily_stats
      [
        PipelineStage.group(
          {
            'year' => PipelineOperator.year('$report_time'),
            'month' => PipelineOperator.month('$report_time'),
            'day' => PipelineOperator.day_of_month('$report_time'),
          },
          changes: PipelineOperator.sum(1)
        ),
        PipelineStage.group(
          nil,
          changesPerDay: PipelineOperator.avg('$changes'),
        )
      ]
    end

    def add_average_change
      PipelineStage.add_fields(
        averageChange: PipelineOperator.avg('$dieselChange', '$e5Change', '$e10Change')
      )
    end

    def match_price_increases
      PipelineStage.match(averageChange: { '$gt' => 0 })
    end

    def match_price_decreases
      PipelineStage.match(averageChange: { '$lt' => 0 })
    end

    def sort_by_timeframe
      PipelineStage.sort(
        'timeframe.hour': 1,
        'timeframe.minute': 1,
      )
    end

    def sort_by_changes
      PipelineStage.sort(
        changes: -1,
        'timeframe.hour': 1,
        'timeframe.minute': 1,
      )
    end

    def shape_output_document
      PipelineStage.add_fields(
        timeframe: '$_id'
      )
    end

    def get_change_count_pipeline(matcher)
      [
        *aggregated_prices_pipeline,
        matcher,
        sort_by_changes,
        PipelineStage.limit(5)
      ]
    end

    def group_data_by_day
      PipelineStage.group(
        {
          'year' => PipelineOperator.year('$report_time'),
          'month' => PipelineOperator.month('$report_time'),
          'day' => PipelineOperator.day_of_month('$report_time'),
        },
        changes: PipelineOperator.sum(1),
        dieselData: PipelineOperator.push(
          {
            'report_timeframe' => '$report_timeframe',
            'price' => '$diesel.price',
            'change' => '$diesel.change'
          }
        ),
        e5Data: PipelineOperator.push(
          {
            'report_timeframe' => '$report_timeframe',
            'price' => '$e5.price',
            'change' => '$e5.change'
          }
        ),
        e10Data: PipelineOperator.push(
          {
            'report_timeframe' => '$report_timeframe',
            'price' => '$e10.price',
            'change' => '$e10.change'
          }
        )
      )
    end

    def get_fuel_difference_pipeline(fuel_type)
      [
        {
          # TODO: Figure out how to pass a variably named param to a double-splat method argument
          '$addFields' => {
            data_field_name(fuel_type) => PipelineOperator.sort_array(
              '$' + data_field_name(fuel_type),
              'price' => 1,
              'report_timeframe.hour' => 1,
              'report_timeframe.minute' => 1,
            )
          }
        },
        {
          '$addFields' => {
            fuel_type => {
              'lowest' => PipelineOperator.first('$' + data_field_name(fuel_type)),
              'highest' => PipelineOperator.last('$' + data_field_name(fuel_type)),
            }
          }
        },
        {
          '$addFields' => {
            data_field_name(fuel_type) => PipelineOperator.sort_array(
              PipelineOperator.map(
                '$' + data_field_name(fuel_type),
                'price_report',
                {
                  'report_timeframe' => '$$price_report.report_timeframe',
                  'higher_than_lowest' => PipelineOperator.subtract(
                    '$$price_report.price',
                    '$$ROOT.' + fuel_type + '.lowest.price',
                  )
                }
              ),
              'report_timeframe.hour' => 1,
              'report_timeframe.minute' => 1,
            )
          }
        },
        PipelineStage.unwind('$' + data_field_name(fuel_type)),
        PipelineStage.group(
          '$' + data_field_name(fuel_type) + '.report_timeframe',
          higherThanLowest: PipelineOperator.avg('$' + data_field_name(fuel_type) + '.higher_than_lowest'),
          times: PipelineOperator.sum(1),
        ),
        PipelineStage.replace_with(
          {
            '_id' => '$_id',
            fuel_type => {
              'higherThanLowest' => '$higherThanLowest',
              'times' => '$times',
              'impact' => PipelineOperator.multiply('$higherThanLowest', '$times'),
            }
          }
        ),
      ]
    end

    def get_diff_pipeline
      [
        match_station,
        group_data_by_day,
        PipelineStage.facet(
          diesel: get_fuel_difference_pipeline('diesel'),
          e5: get_fuel_difference_pipeline('e5'),
          e10: get_fuel_difference_pipeline('e10'),
        ),
        PipelineStage.replace_with(
          {
            'data' => PipelineOperator.concat_arrays('$diesel', '$e5', '$e10')
          }
        ),
        PipelineStage.unwind('$data'),
        PipelineStage.group(
          '$data._id',
          diesel: PipelineOperator.push('$data.diesel'),
          e5: PipelineOperator.push('$data.e5'),
          e10: PipelineOperator.push('$data.e10'),
        ),
        PipelineStage.add_fields(
          diesel: PipelineOperator.first('$diesel'),
          e5: PipelineOperator.first('$e5'),
          e10: PipelineOperator.first('$e10'),
        ),
        PipelineStage.add_fields(
          average: {
            'higherThanLowest' => PipelineOperator.avg(
              '$diesel.higherThanLowest',
              '$e5.higherThanLowest',
              '$e10.higherThanLowest'
            ),
            'times' => PipelineOperator.avg(
              '$diesel.times',
              '$e5.times',
              '$e10.times'
            )
          }
        ),
        PipelineStage.add_fields(
          average: {
            'impact' => PipelineOperator.multiply('$average.higherThanLowest', '$average.times'),
          },
        ),
        shape_output_document
      ]
    end

    def sort_fuel_data_by_price(fuel_type)
      PipelineOperator.sort_array(
        '$' + data_field_name(fuel_type),
        'price' => 1,
        'report_timeframe.hour' => 1,
        'report_timeframe.minute' => 1
      )
    end

    def find_extreme_price_for_fuel(fuel_type)
      {
        'lowestEntry' => PipelineOperator.first('$' + data_field_name(fuel_type)),
        'highestEntry' => PipelineOperator.last('$' + data_field_name(fuel_type)),
      }
    end

    def find_extreme_times_for_fuel(fuel_type)
      {
        'lowestTimes' => PipelineOperator.filter(
          '$' + data_field_name(fuel_type),
          PipelineOperator.eq(
            '$$priceReport.price',
            '$' + fuel_type + '.lowestEntry.price'
          ),
          'priceReport',
        ),
        'highestTimes' => PipelineOperator.filter(
          '$' + data_field_name(fuel_type),
          PipelineOperator.eq(
            '$$priceReport.price',
            '$' + fuel_type + '.highestEntry.price'
          ),
          'priceReport',
        ),
      }
    end

    def group_extreme_times_for_fuel(fuel_type, type, limit)
      [
        PipelineStage.unwind('$' + fuel_type + '.' + type + 'Times'),
        PipelineStage.group(
          '$' + fuel_type + '.' + type + 'Times.report_timeframe',
          count: PipelineOperator.sum(1),
        ),
        PipelineStage.sort(count: -1),
        PipelineStage.limit(limit),
      ]
    end

    def shape_extreme_times_for_fuel(fuel_type)
      PipelineOperator.map(
        '$' + fuel_type,
        'report',
        {
          fuel_type => {
            'timeframe' => '$$report._id',
            'count' => '$$report.count'
          }
        }
      )
    end

    def shape_extreme_times(limit)
      elements = []

      for i in 0..limit-1 do
        elements.push(
          PipelineOperator.merge_objects(
            PipelineOperator.array_elem_at('$diesel', i),
            PipelineOperator.array_elem_at('$e5', i),
            PipelineOperator.array_elem_at('$e10', i),
          )
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
        PipelineStage.add_fields(
          dieselData: sort_fuel_data_by_price('diesel'),
          e5Data: sort_fuel_data_by_price('e5'),
          e10Data: sort_fuel_data_by_price('e10'),
        ),
        PipelineStage.add_fields(
          diesel: find_extreme_price_for_fuel('diesel'),
          e5: find_extreme_price_for_fuel('e5'),
          e10: find_extreme_price_for_fuel('e10'),
        ),
        PipelineStage.add_fields(
          diesel: find_extreme_times_for_fuel('diesel'),
          e5: find_extreme_times_for_fuel('e5'),
          e10: find_extreme_times_for_fuel('e10'),
        ),
        PipelineStage.facet(
          diesel: group_extreme_times_for_fuel('diesel', type, limit),
          e5: group_extreme_times_for_fuel('e5', type, limit),
          e10: group_extreme_times_for_fuel('e10', type, limit),
        ),
        PipelineStage.replace_with(
          {
             'diesel' => shape_extreme_times_for_fuel('diesel'),
             'e5' => shape_extreme_times_for_fuel('e5'),
             'e10' => shape_extreme_times_for_fuel('e10'),
           }
        ),
        PipelineStage.replace_with(shape_extreme_times(limit)),
        PipelineStage.unwind('$data'),
        PipelineStage.replace_with('$data'),
      ]
    end

    def data_field_name(fuel_type)
      fuel_type + 'Data'
    end
end
