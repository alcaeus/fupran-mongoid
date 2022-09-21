require "application_system_test_case"

class PriceReportsTest < ApplicationSystemTestCase
  setup do
    @price_report = price_reports(:one)
  end

  test "visiting the index" do
    visit price_reports_url
    assert_selector "h1", text: "Price reports"
  end

  test "should create price report" do
    visit price_reports_url
    click_on "New price report"

    fill_in "Price diesel", with: @price_report.price_diesel
    check "Price diesel changed" if @price_report.price_diesel_changed
    fill_in "Price e10", with: @price_report.price_e10
    check "Price e10 changed" if @price_report.price_e10_changed
    fill_in "Price e5", with: @price_report.price_e5
    check "Price e5 changed" if @price_report.price_e5_changed
    fill_in "Report time", with: @price_report.report_time
    fill_in "Station uuid", with: @price_report.station_uuid
    click_on "Create Price report"

    assert_text "Price report was successfully created"
    click_on "Back"
  end

  test "should update Price report" do
    visit price_report_url(@price_report)
    click_on "Edit this price report", match: :first

    fill_in "Price diesel", with: @price_report.price_diesel
    check "Price diesel changed" if @price_report.price_diesel_changed
    fill_in "Price e10", with: @price_report.price_e10
    check "Price e10 changed" if @price_report.price_e10_changed
    fill_in "Price e5", with: @price_report.price_e5
    check "Price e5 changed" if @price_report.price_e5_changed
    fill_in "Report time", with: @price_report.report_time
    fill_in "Station uuid", with: @price_report.station_uuid
    click_on "Update Price report"

    assert_text "Price report was successfully updated"
    click_on "Back"
  end

  test "should destroy Price report" do
    visit price_report_url(@price_report)
    click_on "Destroy this price report", match: :first

    assert_text "Price report was successfully destroyed"
  end
end
