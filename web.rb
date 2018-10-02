get "/test" do
  content_type 'application/json'

  { data: { message: "it works!" } }.to_json
end

get "/
error" do
  error "No way!"
end
