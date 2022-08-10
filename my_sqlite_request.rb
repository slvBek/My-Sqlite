require 'csv'

class MySqliteRequest
    def initialize
        @type_of_request = :none
        @select_columns  = []
        @where_params    = []
        @join_params     = []
        @order_params    = []
        @insert_attributes = {}
        @table_name      = nil
        @order           = :asc
    end

    def from(table_name)
        @table_name = table_name
        self
    end

    def select(columns)
        if (columns.is_a?(Array))
            @select_columns += columns.collect { |elem| elem.to_s }
        else
            @select_columns << columns.to_s
        end
        self._setTypeOfRequest(:select)
        self
    end

    def where(column_name, criteria)
        @where_params << [column_name, criteria]
        self
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b)
        @join_params << [column_on_db_a, filename_db_b, column_on_db_b]
        self
    end

    def order(order, column_name)
        @order_params << [order, column_name]
        self
    end

    def insert(table_name)
        self._setTypeOfRequest(:insert)
        @table_name = table_name
        self
    end

    def values(data)
        if (@type_of_request == :insert)
            @insert_attributes = data
        else
            @insert_attributes = data
        end
        self
    end

    def update(table_name)
        self._setTypeOfRequest(:update)
        @table_name = table_name
        self
    end

    def set(data)
        @insert_attributes = data
        self
    end

    def delete
        self._setTypeOfRequest(:delete)
        self
    end

    def write_file(result)
        CSV.open(@table_name, "w", :headers => true) do |b|
            b << result[0].to_hash.keys
            result.each do |bla|
                b << CSV::Row.new(bla.to_hash.keys, bla.to_hash.values)
            end
        end
    end

    def print_insert_type
        puts "Insert Attributes #{@select_columns}"
    end

    def print_select_type(result)
        if !result
            return
        end
        if result.length == 0
            puts "There is no result"
        else
            puts result.first.keys.join(' | ')
            bek = result.first.keys.join(' | ').length
            puts "-_" * bek
            result.each do |line|
                puts line.values.join(' | ')
            end
            puts "-_" * bek
        end
    end

    def print_h(result)
        puts result
        return 
    end

    def run
        print
        if (@type_of_request == :select)
            param = _run_select
            if param == 0
                print_h(param)
            else
                print_select_type(param)
            end
        elsif (@type_of_request == :insert)
            _run_insert
        elsif(@type_of_request == :update)
            update = _run_update
            write_file(update)
        elsif(@type_of_request == :delete)
            delete = _run_delete
            write_file(delete)
        end
    end

    def _setTypeOfRequest(new_type)
        if (@type_of_request == :none || @type_of_request == new_type)
            @type_of_request = new_type
        else
            raise 'Invalid'
        end
    end

    def _run_select
        data = CSV.parse(File.read(@table_name), headers: true)
        result = []
        if(@select_columns != [] && @where_params != [])
            data.each do |elem|
                @where_params.each do |where_attribute|
                    if(elem[where_attribute[0]] == where_attribute[1])
                        result << elem.to_hash.slice(*@select_columns)
                    end
                    x = 0 
                    while x < result.length
                        y = x+1
                        while y < result.length
                            if result[x] == result[y];
                                puts result[x]
                                return 
                            end
                            y+=1
                        end
                        x+=1 
                    end    
                end
            end
        elsif(@select_columns != [] && @where_params == [])
            data.each do |elem|
                @select_columns.each do |sel_col|
                    if elem[sel_col]
                        result << elem.to_hash.slice(*@select_columns)
                    else
                        result << elem.to_hash
                    end
                end
            end
        end
        result
    end

    def _run_insert
        File.open(@table_name, 'a') do |f|
            f.puts @insert_attributes.values.join(',')
        end
    end

    def _run_update
        result = []
        CSV.parse(File.read(@table_name), headers: true).each do |elem|
            @where_params.each do |left, right|
                if right == elem[left]
                    @insert_attributes.each do |left, right|
                        elem[left] = right
                    end
                    result << elem
                else
                    result << elem
                end
            end
        end
        result
    end

    def _run_delete
        result = []
        data = CSV.parse(File.read(@table_name), headers: true)
        result = []
        data.each do |elem|
            @where_params.each do |left, right|
                if right == elem[left]
                    next
                else
                    result << elem
                end
            end
        end
        result
    end 
end