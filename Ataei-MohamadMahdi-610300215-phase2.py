import csv
def cheack_exist_condition_column(db_name,lines , condition_column): #Checks whether the condition column exists or not
    for line in lines: #Finding the condition column and extracting its type
        line_split = line.split()
        if(line_split[0]==condition_column):
            return 1
    else:
        return 0
def find_type(lines,column): #Extracts the type of the desired column
    for line in lines:
        line_split=line.split()
        if(line_split[0]==column):
            type = line_split[1]
            break
    return type
def check_insert_type(list_record,lines): #Checks the typing of the input values by typing the corresponding column
    for i in range(len(list_record)):
        if not (type_matching(find_type(lines,lines[i].split()[0]),list_record[i])):
            return 0
    else:
        return 1
def type_matching (type,value): #It checks whether the type of our string is equal to the input type or not
    try :
        int(value)
        if(type=="INT"):
            return 1
    except:
        try:
            float(value)
            if(type=="FLOAT"):
                return 1
        except:
            if(type=="STRING"):
                return 1
    return 0
def dt_base_schema(db_name) : #It separates the name and type of columns of a table from the schema file
    try:
        flag = 0
        with open('schema.txt', 'r') as schema:
            lines = schema.readlines()
            for i in range(len(lines)) : # Finding row numbers of a database
                lines[i] = lines[i].rstrip()  # remove '\n' at end of line
                if(lines[i]==db_name):
                    flag = 1
                    first_row_index= int(i+1)
                    continue
                if(lines[i]=="" and flag==1):
                    end_row_index = i
                    break
        return (lines[first_row_index:end_row_index])
    except :
        print("Error in %s table: We do not have such a database " % (db_name))
        return 0
def bigger_select (condition , db_name , selected_rows , lines): # bigger and smaller and draw cheack if and same function
    pos = condition.find('>') #find position > in string
    condition_column = condition[0:pos] #separating name of cc7olumn that cheacking
    condition = condition[pos+1:] #right side of if
    if not cheack_exist_condition_column(db_name,lines,condition_column):#Checks the condition column exists or not
        print("Error in condition column : %s is not exist "  % (condition_column))
        return 0
    type = find_type(lines,condition_column) #We extract the condition column type
    if not type_matching(type,condition): #Match the type of the condition column and the condition
        print("Error condition type : type %s is %s The type of condition and input column should be this type" % (condition_column,type))
        return 0
    with open(db_name+'.csv' , 'r') as file:
        records = csv.DictReader(file)
        global headers
        headers = records.fieldnames
        for row in records :
            if(type=="INT"):
                if(int(row[condition_column])> int(condition)):
                    selected_rows.append(dict(row))
            if(type=="STRING"):
                if(row[condition_column]>condition[1:-1]):
                    selected_rows.append(dict(row))
            if (type=="FLOAT"):
                if (float(row[condition_column]) > float(condition)):
                    selected_rows.append(dict(row))
        file.close()
        return 1 # bigger and smaller and draw cheack if and same function
def smaller_select (condition , db_name , selected_rows,lines): ## bigger and smaller and draw cheack if and same function
    pos = condition.find('<')  # find position > in string
    condition_column = condition[0:pos]  # separating name of column that cheacking
    condition = condition[pos + 1:]  # right side of if
    if not cheack_exist_condition_column(db_name,lines,condition_column):
        print("Error in condition column : %s is not exist " % (condition_column))
        return 0
    type = find_type(lines,condition_column)
    if not type_matching(type, condition):
        print("Error condition type : type %s is %s The type of condition and input column should be this type" % (condition_column,type))
        return 0
    with open(db_name + '.csv', 'r') as file:
        records = csv.DictReader(file)
        global headers
        headers = records.fieldnames
        for row in records:
            if (type == "INT"):
                if (int(row[condition_column]) < int(condition)):
                    selected_rows.append(dict(row))
            if (type == "STRING"):
                if (row[condition_column] < condition[1:-1]):
                    selected_rows.append(dict(row))
            if (type == "FLOAT"):
                if (float(row[condition_column]) < float(condition)):
                    selected_rows.append(dict(row))
        file.close()
        return 1
def draw_select (condition , db_name , selected_rows,lines): # bigger and smaller and draw cheack if and same function
    pos = condition.find('=')  # find position > in string
    condition_column = condition[0:pos]  # separating name of column that cheacking
    condition = condition[pos + 1:]  # right side of if
    if not cheack_exist_condition_column(db_name, lines, condition_column):
        print("Error in condition column : %s is not exist " % (condition_column))
        return 0
    type = find_type(lines,condition_column)
    if not type_matching(type,condition):
        print("Error condition type : type %s is %s The type of condition and input column should be this type" % (condition_column,type))
        return 0
    with open(db_name + '.csv', 'r' , encoding="utf8") as file:
        records = csv.DictReader(file)
        global headers
        headers = records.fieldnames
        for row in records:
            try:
                if (type == "INT"):
                    if (int(row[condition_column]) == int(condition)):
                        selected_rows.append(dict(row))
                if (type == "STRING"):
                    if (condition[1:-1] in row[condition_column]):
                        selected_rows.append(dict(row))
                if (type == "FLOAT"):
                    if (float(row[condition_column]) == float(condition)):
                        selected_rows.append(dict(row))
            except:
                continue
        file.close()
        return 1
def create(order) : # create new file
    db_name = order[2] #Separating database names
    try : #If there was already a database with this name, it will give an error
        with open(db_name + '.csv', 'r', newline='')as file :
            print("the %s table already exist please choose other name" % (db_name))
            file.close()
        return 0
    except:
        print(end="")
    string_columns=order[3][1:-1] #Separating columns name with data type from order
    type_name_columns = string_columns.split(',')
    list_type_name = [type_name_columns[i].split() for i in range(len(type_name_columns))]
    columns_name = [name[0] for name in list_type_name ] # Separating the name from the type
    try : #It is checked that each column has a specific name + type
        columns_type = []
        for name in list_type_name:
            columns_type.append(name[1]) #if for each coulmn type not exist give Error
    except :
        print("Error in define column %s : Column name + type must be entered " %(name[0]))
        return 0
    for types in columns_type : #It checks that the type of columns is not out of int, float and string
        if types!="int" and types!="float" and types != "string" :
            print("Error in identity %s : This type of data is not defined" % (types))
            return 0
    with open('schema.txt' , 'a' , newline='' ) as schema : # create schema and write on name and type columns
        schema.write(db_name + '\n')
        for column in list_type_name :
            schema.write(column[0] + ' ' + column[1].upper() + '\n')
        schema.write('\n')
        schema.close()
    with open(db_name+'.csv', 'w', newline='') as file: #write name column in file as feildname
        writer = csv.writer(file)
        writer.writerow(columns_name)
        file.close()
    return 1
def insert_into(order): #
    db_name = order[2] #database name
    list_record = order[4][1:-1].split(',') #delet () from value string
    try: #It checks whether the written database name exists or not
        with open(db_name + '.csv', 'r', newline='') as file:
            file.close()
    except :
        print("Error in open CSV file : We do not have such a %s table " % (db_name))
        return 0
    lines = dt_base_schema(db_name) #It is the name and type of each table column
    if(len(lines)!=len(list_record)): #It checks that the number of input values is equal to the number of columns in the table
        print("Error in value number : your number of value must be %d" %len(lines))
        return 0
    if not check_insert_type(list_record,lines): # #Checks the typing of the input values by typing the corresponding column
        print("Error in type inserted value")
        return 0
    for i in range(len(list_record)): #delet "" from string data
        if(list_record[i][0]=='"'):
            list_record[i]=list_record[i][1:-1]
    with open(db_name + '.csv', 'a', newline='') as file:  # open and write record as row in file
        writer = csv.writer(file)
        writer.writerow(list_record)
        file.close()
    return 1
def select(order):
    selected_rows = []
    db_name  , condition = order[3] , order[5]
    lines = dt_base_schema(db_name)
    if(condition.find('>')!= -1):
        if not bigger_select(condition,db_name , selected_rows,lines):
            return 0
    if (condition.find('<') != -1):
        if not smaller_select(condition,db_name, selected_rows,lines):
            return 0
    if (condition.find('=') != -1):
        if not draw_select(condition,db_name , selected_rows,lines):
            return 0
    if (order[1] != "*"): #print all columns
        selected_columns = order[1][1:-1].split(',')
        for selected_column in selected_columns: #Checks whether the called column exists in the table or not
            if (selected_column not in headers):
                print("Error in selected columns : %s not exist in feildname" %(selected_column))
                return 0
        for column in headers: #The uncalled column is deleted
            if (column not in selected_columns):
                for selected_row in selected_rows:
                    del selected_row[column]
    for selected_row in selected_rows:
        print(selected_row)
    return 1
while 1 :
    try : #must type of number order be int
        number_order  = int(input())  # Number of order lines
        break
    except :
        print("First enter the number of order(int number)")
counter = 0
while(counter<number_order):
    order = input()  # user order for database
    split_order = order.split()  # split order words
    if (split_order[0]+ ' ' + split_order[1]=='CREATE TABLE'):
        order = order.split(' ', 3)
        if not create(order): #If for any reason the command is not executed, it will be repeated again
            continue
    elif (split_order[0]+ ' ' + split_order[1] == 'INSERT INTO'):
        order = order.split(' ', 4)
        if not insert_into(order):#If for any reason the command is not executed, it will be repeated again
            continue
    elif(split_order[0]=='SELECT'):
        order=order.split(' ' , 5)
        if not select(order):#If for any reason the command is not executed, it will be repeated again
            continue
    else :
        print("Error in commad : We do not have such a command please choose (CREATE TABLE , INSERT INTO , SELECT)") #Command spelling error
        continue
    counter += 1

