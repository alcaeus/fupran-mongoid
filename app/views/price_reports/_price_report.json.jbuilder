json.extract! price_report, :id, :report_time, :station_uuid, :price_diesel, :price_e5, :price_e10, :price_diesel_changed, :price_e5_changed, :price_e10_changed, :created_at, :updated_at
json.url price_report_url(price_report, format: :json)
