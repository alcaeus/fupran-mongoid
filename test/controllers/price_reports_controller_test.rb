require "test_helper"

class PriceReportsControllerTest < ActionDispatch::IntegrationTest
  setup do
    @price_report = price_reports(:one)
  end

  test "should get index" do
    get price_reports_url
    assert_response :success
  end

  test "should get new" do
    get new_price_report_url
    assert_response :success
  end

  test "should create price_report" do
    assert_difference("PriceReport.count") do
      post price_reports_url, params: { price_report: { price_diesel: @price_report.price_diesel, price_diesel_changed: @price_report.price_diesel_changed, price_e10: @price_report.price_e10, price_e10_changed: @price_report.price_e10_changed, price_e5: @price_report.price_e5, price_e5_changed: @price_report.price_e5_changed, report_time: @price_report.report_time, station_uuid: @price_report.station_uuid } }
    end

    assert_redirected_to price_report_url(PriceReport.last)
  end

  test "should show price_report" do
    get price_report_url(@price_report)
    assert_response :success
  end

  test "should get edit" do
    get edit_price_report_url(@price_report)
    assert_response :success
  end

  test "should update price_report" do
    patch price_report_url(@price_report), params: { price_report: { price_diesel: @price_report.price_diesel, price_diesel_changed: @price_report.price_diesel_changed, price_e10: @price_report.price_e10, price_e10_changed: @price_report.price_e10_changed, price_e5: @price_report.price_e5, price_e5_changed: @price_report.price_e5_changed, report_time: @price_report.report_time, station_uuid: @price_report.station_uuid } }
    assert_redirected_to price_report_url(@price_report)
  end

  test "should destroy price_report" do
    assert_difference("PriceReport.count", -1) do
      delete price_report_url(@price_report)
    end

    assert_redirected_to price_reports_url
  end
end
