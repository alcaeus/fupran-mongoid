class PriceReportsController < ApplicationController
  before_action :set_price_report, only: %i[ show ]

  # GET /price_reports or /price_reports.json
  def index
    @price_reports = PriceReport.order(:_id.asc).page params[:page]
  end

  # GET /price_reports/1 or /price_reports/1.json
  def show
  end

  private
    # Use callbacks to share common setup or constraints between actions.
    def set_price_report
      @price_report = PriceReport.find(params[:id])
    end
end
