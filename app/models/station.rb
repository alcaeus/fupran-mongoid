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

  def aggregated_prices
    PriceReport.collection.aggregate(
      [
        match_station,
        group_by_timeframe,
        sort_by_timeframe,
        shape_output_document
      ]
    ).to_a
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

    def sort_by_timeframe
      {
        '$sort' => {
          '_id.hour' => 1,
          '_id.minute' => 1
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
end
