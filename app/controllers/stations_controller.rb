class StationsController < ApplicationController
  before_action :set_station, only: %i[ show ]

  # GET /stations or /stations.json
  def index
    @stations = Station.where(post_code: 82140)
    # @stations = Station.all
    # @stations = Station.order(:uuid.asc).page params[:page]
  end

  # GET /stations/1 or /stations/1.json
  def show
  end

  private
    # Use callbacks to share common setup or constraints between actions.
    def set_station
      @station = Station.find(params[:id])
    end
end
