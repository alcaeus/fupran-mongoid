class PriceReport
  include Mongoid::Document

  belongs_to :station, inverse_of: :price_reports

  field :report_time, type: Time

  embeds_one :diesel, class_name: "Price"
  embeds_one :e5, class_name: "Price"
  embeds_one :e10, class_name: "Price"
end
