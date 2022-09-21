require 'csv'

def only_without_previous_report
  {
    '$match' => {
      'previous_price_report' => {
        '$exists' => false
      }
    }
  }
end

def embed_previous_report
  [
    {
      '$lookup' => {
        'from' => 'price_reports',
        'localField' => 'station_id',
        'foreignField' => 'station_id',
        'as' => 'previous_price_report',
        'let' => {
          'current_report_time' => '$report_time'
        },
        'pipeline' => [
          {
            '$match' => {
              '$expr' => {
                '$lt' => [
                  '$report_time', '$$current_report_time'
                ]
              }
            }
          },
          {
            '$sort' => {
              'report_time' => -1
            }
          },
          {
            '$limit' => 1
          },
          {
            '$unset' => 'previous_price_report'
          }
        ]
      }
    },
    {
      '$set' => {
        'previous_price_report' => {
          '$first' => '$previous_price_report'
        }
      }
    }
  ]
end

def calculate_price_difference
  {
    '$addFields' => {
      'timeValid' => {
        '$cond' => {
          'if' => {
            '$ne' => [
              '$previous_price_report', nil
            ]
          },
          'then' => {
            '$dateDiff' => {
              'unit' => 'second',
              'startDate' => '$previous_price_report.report_time',
              'endDate' => '$report_time'
            }
          },
          'else' => nil
        }
      },
      'diesel' => {
        'change' => {
          '$cond' => {
            'if' => {
              '$ne' => [
                '$previous_price_report', nil
              ]
            },
            'then' => {
              '$subtract' => [
                '$diesel.price', '$previous_price_report.diesel.price'
              ]
            },
            'else' => nil
          }
        }
      },
      'e5' => {
        'change' => {
          '$cond' => {
            'if' => {
              '$ne' => [
                '$previous_price_report', nil
              ]
            },
            'then' => {
              '$subtract' => [
                '$e5.price', '$previous_price_report.e5.price'
              ]
            },
            'else' => nil
          }
        }
      },
      'e10' => {
        'change' => {
          '$cond' => {
            'if' => {
              '$ne' => [
                '$previous_price_report', nil
              ]
            },
            'then' => {
              '$subtract' => [
                '$e10.price', '$previous_price_report.e10.price'
              ]
            },
            'else' => nil
          }
        }
      },
      'report_timeframe' => {
        'hour' => {
          '$hour' => '$report_time'
        },
        'minute' => {
          '$multiply' => [
            5, {
            '$floor' => {
              '$divide' => [
                {
                  '$minute' => '$report_time'
                }, 5
              ]
            }
          }
          ]
        }
      }
    }
  }
end

def embed_station
  [
    {
      '$lookup' => {
        'from' => 'stations',
        'localField' => 'station_id',
        'foreignField' => '_id',
        'as' => 'station'
      }
    }, {
      '$set' => {
        'station' => {
          '$first' => '$station'
        }
      }
    }
  ]
end

def merge_into_collection
  {
    '$merge' => {
      'into' => PriceReport.collection_name.to_s,
      'whenNotMatched' => 'discard'
    }
  }
end

namespace :app do
  desc "Loads sample data into database (using Mongoid; slow)"
  task load_data_mongoid: :environment do
    Station.delete_all
    PriceReport.delete_all

    stations = Hash.new

    CSV.foreach(Rails.root.join('data/stations-sample.csv'), headers: false) do |row|
      station_uuid = row[0]

      stations[station_uuid] = Station.create({
        _id: station_uuid,
        name: row[1],
        brand: row[2],
        street: row[3],
        house_number: row[4],
        post_code: row[5],
        place: row[6],
        latitude: row[7],
        longitude: row[8],
      })
    end

    CSV.foreach(Rails.root.join('data/prices-sample.csv'), headers: false) do |row|
      station_uuid = row[1]

      PriceReport.create({
        report_time: row[0],
        station: stations[station_uuid],
        diesel: Price.new({
          price: row[2].to_f * 1000,
          price_changed: row[5],
        }),
        e5: Price.new({
          price: row[3].to_f * 1000,
          price_changed: row[6],
        }),
        e10: Price.new({
          price: row[4].to_f * 1000,
          price_changed: row[7],
        }),
      })
    end
  end

  desc "Loads sample data into database (using driver; faster)"
  task load_data_driver: :environment do
    Station.delete_all
    PriceReport.delete_all

    # CSV.foreach(Rails.root.join('data/stations-sample.csv'), headers: false) do |row|
    CSV.foreach(Rails.root.join('data/stations.csv'), headers: true) do |row|
      station_uuid = row[0]

      Station.collection.insert_one({
        _id: station_uuid,
        name: row[1],
        brand: row[2],
        street: row[3],
        house_number: row[4].to_i,
        post_code: row[5].to_i,
        place: row[6],
        latitude: row[7].to_f,
        longitude: row[8].to_f
      })
    end

    # CSV.foreach(Rails.root.join('data/prices-sample.csv'), headers: false) do |row|
    CSV.foreach(Rails.root.join('data/prices.csv'), headers: false) do |row|
      station_uuid = row[1]

      PriceReport.collection.insert_one({
        report_time: row[0].to_datetime,
        station_id: station_uuid,
        diesel: {
          price: row[2].to_f * 1000,
          price_changed: (!!row[5]),
        },
        e5: {
          price: row[3].to_f * 1000,
          price_changed: (!!row[6]),
        },
        e10: {
          price: row[4].to_f * 1000,
          price_changed: (!!row[7]),
        }
      })
    end
  end

  desc "Materialize price report view"
  task materialize_view: :environment do
    result = PriceReport.collection.aggregate(
      [
        only_without_previous_report,
        *embed_previous_report,
        calculate_price_difference,
        *embed_station,
        merge_into_collection
      ]
    ).to_a
  end
end
