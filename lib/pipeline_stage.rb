module PipelineStage
  def self.add_fields(**fields)
    {
      '$addFields' => fields,
    }
  end

  def self.facet(**field_pipelines)
    {
      '$facet' => field_pipelines,
    }
  end

  def self.group(id, **fields)
    {
      '$group' => {
        '_id' => id,
        **fields,
      }
    }
  end

  def self.match(**matchers)
    { '$match' => matchers }
  end

  def self.limit(limit)
    { '$limit' => limit }
  end

  def self.replace_with(document)
    { '$replaceWith' => document }
  end

  def self.sort(**fields)
    { '$sort' => fields }
  end

  def self.unwind(path, include_array_index = '', preserve_null_and_empty_arrays = false)
    data = {
      path: path,
      preserveNullAndEmptyArrays: preserve_null_and_empty_arrays,
    }

    if !include_array_index.blank?
      data['include_array_index'] = include_array_index;
    end

    { '$unwind' => data }
  end
end
