require 'readline'
require_relative "my_sqlite_request"

def readline_management
    buf = Readline.readline('sqlite3> ', true)
    return buf
end

def array_hash(arr)
    result = Hash.new
    i = 0
    while i < arr.length
        left, right = arr[i].split("=")
        result[left] = right
        i += 1
    end
    result
end

def action_case(action, args, request)
    case action
    when "from"
        if args.length != 1
            puts "Ex.: FROM nba_player_data.csv"
            return
        else
            request.from(*args)
        end

    when "select"
        if args.length < 1
            puts "Ex.: SELECT name, age"
            return
        else
            request.select(args)
        end

    when "where"
        if args.length != 1
            puts "Ex.: WHERE age=20"
        else
            col, val = args[0].split("=")
            request.where(col, val)
        end

    when "order"
        if args.length != 2
            p "Ex.: ORDER age ASC"
        else
            col_name = args[0]
            sort_type = args[1].downcase
            request.order(sort_type, col_name)
        end 

    when "insert"
        if args.length != 1
            puts "Ex.: INSERT nba_player_data.csv. Use VALUES"
        else
            request.insert(*args)
        end

    when "values"
        if args.length < 1
            puts "Provide some data to insert. Ex.: name=Bekzat, birth_state=CA, age=90"
        else
            request.values(array_hash(args))
        end

    when "update"
        if args.length != 1
            puts "Ex.: UPDATE nba_player_data.csv"
        else
            request.update(*args)
        end

    when "set"
        if args.length < 1
            puts "Ex.: SET name=Bekzat. Use WHERE - otherwise WATCH OUT."
        else
            request.set(array_hash(args))
        end

    when "delete"
        if args.length != 0
            puts "Ex.: DELETE FROM nba_player_data.csv! Use WHERE - otherwise WATCH OUT."
        else
            request.delete
        end
    end
end

def to_do_request(sql)
    valid_methods = ["SELECT", "FROM", "WHERE", "ORDER", "INSERT", "VALUES", "UPDATE", "SET", "DELETE"]
    command = nil
    args = []
    request = MySqliteRequest.new
    splited_command = sql.split(" ")

    0.upto splited_command.length - 1 do |arg|
        if valid_methods.include?(splited_command[arg].upcase())
            if (command != nil)
                if command != "..."
                    args = args.join(" ").split(", ")
                end
                action_case(command, args, request)
                command = nil
                args = []
            end
            command = splited_command[arg].downcase()
        else
            args << splited_command[arg]
        end
    end
    if args[-1].end_with?(";")
        args[-1] = args[-1].chomp(";")
        action_case(command, args, request)
        request.run
    else
        p "End request ;"
    end
end

def run
    puts "MySQLite version 0.3 2022-04-14\nIf you need help, type .help" 
    while command = readline_management
        if command == "quit"
            break
        elsif command == ".help"
            puts "***********************    REQUEST     ******************************"
            puts "SELECT name FROM nba_player_data.csv;\nSELECT * FROM nba_player_data.csv;\nINSERT nba_player_data.csv VALUES name=Bekzat year_start=2020 year_end=not_clear position=F-C height=6-10 weight=240 birth_date=\"05.02.2004\" college=Astrum;\nUPDATE nba_player_data.csv SET name=Bek WHERE year_start=2020;"
            puts "DELETE FROM nba_player_data.csv WHERE year_start=2020;\nDELETE FROM nba_player_data.csv;\nquit;\n"
        else
            to_do_request(command)
        end
    end
end

run()