require 'json'

class Place
  include ActiveModel::Model
  attr_accessor :id, :formatted_address, :location, :address_components

  def initialize(params)
    @places = Place.collection
    @id = params[:_id].to_s
    @formatted_address = params[:formatted_address]
    @location = Point.new(params[:geometry][:geolocation])
    @address_components = []

    if params[:address_components]
      params[:address_components].each do |address_component|
        @address_components << AddressComponent.new(address_component)
      end
    end

  end

  def persisted?
    !@id.nil?
  end

  def self.mongo_client
    Mongoid::Clients.default
  end

  def self.collection
    mongo_client[:places]
  end

  def self.load_all(file)
    collection.insert_many(JSON.parse(file.read))
  end

  def self.find_by_short_name(short_name)
    Place.collection.find({"address_components.short_name" => short_name})
  end

  def self.to_places(places)
    places.map{|place| Place.new(place)}
  end

  def self.find(id)
    place = collection.find({:_id => BSON::ObjectId.from_string(id)}).first
    Place.new(place) unless place.nil?
  end

  def self.all(offset = 0, limit = 0)
    places = collection.find.skip(offset).limit(limit)
    to_places(places)
  end

  def destroy
    @places.find(:_id => BSON::ObjectId.from_string(@id)).delete_one
  end

  def self.get_address_components(sort = nil, offset = 0, limit = nil)
    if sort.nil? and limit.nil?
      Place.collection.aggregate([
          {:$unwind => "$address_components"},
          {:$project => {:_id => 1, :address_components => 1, :formatted_address => 1, :geometry => {:geolocation => 1}}},
          {:$skip => offset}
                                 ])
    elsif sort.nil? and !limit.nil?
      Place.collection.aggregate([
          {:$unwind => "$address_components"},
          {:$project => {:_id => 1, :address_components => 1, :formatted_address => 1, :geometry => {:geolocation => 1}}},
          {:$skip => offset},
          {:$limit => limit}
                                 ])
    elsif !sort.nil? and limit.nil?
      Place.collection.aggregate([
          {:$unwind => "$address_components"},
          {:$project => {:_id => 1, :address_components => 1, :formatted_address => 1, :geometry => {:geolocation => 1}}},
          {:$sort => sort},
          {:$skip => offset}
                                 ])
    else
      Place.collection.aggregate([
          {:$unwind => "$address_components"},
          {:$project => {:_id => 1, :address_components => 1, :formatted_address => 1, :geometry => {:geolocation => 1}}},
          {:$sort => sort},
          {:$skip => offset},
          {:$limit => limit}
                                 ])
    end
  end

  def self.get_country_names
    Place.collection.aggregate([
          {:$unwind => '$address_components'},
          {:$project=>{ :_id => 0, :address_components => {:long_name => 1, :types => 1} }},
          {:$match => {'address_components.types': "country"  }}, {:$group=>{ :_id => "$address_components.long_name", :count=>{:$sum=>1}}}
                               ]).to_a.map {|h| h[:_id]}
  end

  def self.find_ids_by_country_code(country_code)
    Place.collection.aggregate([
          {:$unwind => '$address_components'},
          {:$project=>{:_id=>1, :address_components => {:short_name => 1, :types => 1} }},
          {:$match => {'address_components.short_name': country_code}}
                               ]).to_a.map {|h| h[:_id].to_s}
  end

  def self.create_indexes
    Place.collection.indexes.create_one("geometry.geolocation" => Mongo::Index::GEO2DSPHERE)
  end

  def self.remove_indexes
    Place.collection.indexes.drop_one("geometry.geolocation_2dsphere")
  end

  def self.near(point, max_meters = 0)
    collection.find('geometry.geolocation' => {:$near => {:$geometry => point.to_hash, :$maxDistance => max_meters}})
  end

  def near(max_meters = 0)
    Place.to_places(Place.near(@location.to_hash, max_meters))
  end

  def photos(offset = 0, limit = 0)
    photos = Photo.find_photos_for_place(@id).skip(offset).limit(limit)
    photos.map {|photo| Photo.new(photo)}
  end
  
end
