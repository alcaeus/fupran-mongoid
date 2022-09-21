require "application_system_test_case"

class StationsTest < ApplicationSystemTestCase
  setup do
    @station = stations(:one)
  end

  test "visiting the index" do
    visit stations_url
    assert_selector "h1", text: "Stations"
  end

  test "should create station" do
    visit stations_url
    click_on "New station"

    fill_in "Brand", with: @station.brand
    fill_in "House number", with: @station.house_number
    fill_in "Latitude", with: @station.latitude
    fill_in "Longitude", with: @station.longitude
    fill_in "Name", with: @station.name
    fill_in "Place", with: @station.place
    fill_in "Post code", with: @station.post_code
    fill_in "Street", with: @station.street
    fill_in "Uuid", with: @station.uuid
    click_on "Create Station"

    assert_text "Station was successfully created"
    click_on "Back"
  end

  test "should update Station" do
    visit station_url(@station)
    click_on "Edit this station", match: :first

    fill_in "Brand", with: @station.brand
    fill_in "House number", with: @station.house_number
    fill_in "Latitude", with: @station.latitude
    fill_in "Longitude", with: @station.longitude
    fill_in "Name", with: @station.name
    fill_in "Place", with: @station.place
    fill_in "Post code", with: @station.post_code
    fill_in "Street", with: @station.street
    fill_in "Uuid", with: @station.uuid
    click_on "Update Station"

    assert_text "Station was successfully updated"
    click_on "Back"
  end

  test "should destroy Station" do
    visit station_url(@station)
    click_on "Destroy this station", match: :first

    assert_text "Station was successfully destroyed"
  end
end
