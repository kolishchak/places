class Photo
  attr_accessor :id, :location, :place
  attr_writer :contents

  def initialize(params = nil)
    @id = params[:_id].to_s if !params.nil? && !params[:_id].nil?
    @location = Point.new(params[:metadata][:location]) if !params.nil? && !params[:metadata].nil?
    @place = params[:metadata][:place] if !params.nil? && !params[:metadata].nil?
  end

  def self.mongo_client
    Mongoid::Clients.default
  end

  def persisted?
    !@id.nil?
  end

  def save
    if !persisted?
      gps = EXIFR::JPEG.new(@contents).gps
      location = Point.new(lng: gps.longitude, lat: gps.latitude)
      @contents.rewind
      description = {}
      description[:metadata] = {:location => location.to_hash, :place => @place}
      description[:content_type] = "image/jpeg"
      @location = Point.new(location.to_hash)
      grid_file = Mongo::Grid::File.new(@contents.read, description)
      @id = Place.mongo_client.database.fs.insert_one(grid_file).to_s
    else
      doc = Photo.mongo_client.database.fs.find({:_id => BSON::ObjectId.from_string(@id)}).first
      doc[:metadata][:place] = @place
      doc[:metadata][:location] = @location.to_hash
      Photo.mongo_client.database.fs.find({:_id => BSON::ObjectId.from_string(@id)}).update_one(doc)
    end
  end

  def self.all(offset = 0, limit = 0)
    mongo_client.database.fs.find.skip(offset).limit(limit).map {|doc| Photo.new(doc)}
  end

  def self.find(id)
    doc = mongo_client.database.fs.find(:_id => BSON::ObjectId.from_string(id)).first
    doc.nil? ? nil : photo = Photo.new(doc)
  end

  def contents
    file = Photo.mongo_client.database.fs.find_one({:_id => BSON::ObjectId.from_string(@id)})
    if file
      buffer = ""
      file.chunks.reduce([]) do |x, chunk|
        buffer << chunk.data.data
      end
      return buffer
    end
  end

  def destroy
    Photo.mongo_client.database.fs.find({:_id => BSON::ObjectId.from_string(@id)}).delete_one
  end

  def find_nearest_place_id(max_meters)
    Place.collection.find({"geometry.geolocation" => {:$near => @location.to_hash}}).limit(1).projection({:_id=>1}).first[:_id]
  end

  def place
    Place.find(@place.to_s) unless @place.nil?
  end

  def place= object
    @place = object
    @place = BSON::ObjectId.from_string(object) if object.is_a? String
    @place = BSON::ObjectId.from_string(object.id) if object.respond_to? :id
  end

  def self.find_photos_for_place(place_id)
    mongo_client.database.fs.find({"metadata.place" => BSON::ObjectId.from_string(place_id.to_s)})
  end

end

