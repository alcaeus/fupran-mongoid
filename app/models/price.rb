class Price
  include Mongoid::Document

  field :price, type: Integer
  field :price_changed, type: Mongoid::Boolean

  embedded_in :price_report, inverse_of: :diesel
  embedded_in :price_report, inverse_of: :e5
  embedded_in :price_report, inverse_of: :e10
end
