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

  def aggregated_prices_pipeline
    [
      match_station,
      group_by_timeframe,
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

    def group_by_timeframe
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
end
