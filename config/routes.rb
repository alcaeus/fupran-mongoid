Rails.application.routes.draw do
  resources :price_reports, only: [:show, :index]
  resources :stations, only: [:show, :index]
  # Define your application routes per the DSL in https://guides.rubyonrails.org/routing.html

  # Defines the root path route ("/")
  root "stations#index"
end
