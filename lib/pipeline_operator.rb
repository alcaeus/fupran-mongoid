module PipelineOperator
  def self.avg(*values)
    values.count == 1 ? value_operator('avg', values[0]) : array_operator('avg', values)
  end

  def self.array_elem_at(value, index)
    array_operator('arrayElemAt', [value, index])
  end

  def self.concat_arrays(*values)
    array_operator('concatArrays', values)
  end

  def self.day_of_month(value)
    value_operator('dayOfMonth', value)
  end

  def self.eq(value1, value2)
    array_operator('eq', [value1, value2])
  end

  def self.filter(input, cond, as = 'this', limit = nil)
    object_operator(
      'filter',
      input: input,
      cond: cond,
      as: as,
      limit: limit,
    )
  end

  def self.first(value)
    value_operator('first', value)
  end

  def self.last(value)
    value_operator('last', value)
  end

  def self.map(input, as = 'this', in_expression)
    object_operator(
      'map',
      input: input,
      as: as,
      in: in_expression
    )
  end

  def self.merge_objects(*values)
    array_operator('mergeObjects', values)
  end

  def self.month(value)
    value_operator('month', value)
  end

  def self.multiply(*values)
    array_operator('multiply', values)
  end

  def self.push(document)
    value_operator('push', document)
  end

  def self.sort_array(value, **sort)
    object_operator(
      'sortArray',
      input: value,
      sortBy: sort
    )
  end

  def self.subtract(value1, value2)
    array_operator('subtract', [value1, value2])
  end

  def self.sum(value)
    value_operator('sum', value)
  end

  def self.year(value)
    value_operator('year', value)
  end

  private

  def self.array_operator(operator, args)
    { '$' + operator => args }
  end

  def self.object_operator(operator, **fields)
    { '$' + operator => fields }
  end

  def self.value_operator(operator, value)
    { '$' + operator => value }
  end
end
