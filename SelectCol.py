def rowTodict(format_, row):
    return_col = []
    for ref in row:
        temp_dict = dict()
        temp = ref.split(":")
        for index in range(len(temp)):
            temp_dict[format_[index]] = temp[index]
        return_col.append(temp_dict)
    return return_col

def dictToFormat(col_value, d_format):
    result_return = []
    for temp in col_value:
        temp_col = []
        for keys in d_format:
            if keys in temp:
                temp_col.append(temp[keys])
            else:
                temp_col.append(".")
        result_return.append(":".join(temp_col))
    return tuple(result_return)

def selectCol(row, lhs_len, rhs_len):
    # INFO re      
    AC, AN = 0, 0 
    
    if row[9] == None :
        GT = row[lhs_len + 9:]
    elif row[lhs_len + 9] == None :
        GT = row[9:lhs_len]
    else:
        GT = row[9:lhs_len]+row[lhs_len + 9:]
        
    for temp in GT:
        if temp == None:
            break
        else:
            if "0/1:" in temp:
                AC += 1
                AN += 1
            elif "1/1:" in temp:
                AC += 2
                AN += 1
            elif "0/0:" in temp:
                AN += 1
    
    # rhs가 null
    if(row.CHROM_temp == None):
        temp = tuple()
        for ref in range(rhs_len - 9):
            temp += ("0/0",) # GC
            AN += 1

        # info
        AN *= 2
        info = ("AC="+str(AC)+";AN="+str(AN)+";SF=0",)
        return row[:5] + (float(row.QUAL),) + (row.FILTER, ) + info + (row[8],) + row[9:lhs_len] + temp
    
    # lhs가 null
    elif(row.CHROM == None):
        temp = tuple()
        for ref in range(lhs_len - 9):
            temp += ("0/0",) # GC
            AN += 1
           
        # info
        AN *= 2
        info = ("AC="+str(AC)+";AN="+str(AN)+";SF=1",)
        return row[lhs_len:lhs_len + 5] + (float(row.QUAL_temp), ) + (row.FILTER_temp, ) + info + (row.FORMAT_temp,) + temp + row[lhs_len + 9:]
    
    # case, control 둘다 존재
    else:
        
        # QUAL re-calculation
        format_, lhs_format, rhs_format = row[8].split(":")+row[lhs_len + 8].split(":"), row[8].split(":"), row[lhs_len + 8].split(":")
        dup_format, lhs_col, rhs_col = [], rowTodict(lhs_format, row[9:lhs_len]), rowTodict(rhs_format, row[lhs_len + 9:])
        
        # format duplicate
        for dup in format_:
            if dup not in dup_format:
                dup_format.append(dup)
        
        result_lhs, result_rhs = dictToFormat(lhs_col, dup_format), dictToFormat(rhs_col, dup_format)
        
        # qual re-calcualtion # 100
        col_total = lhs_len + rhs_len - 18
        lhs_QUAL = float(row.QUAL) * ((lhs_len - 9) / col_total)
        rhs_QUAL = float(row.QUAL_temp) * ((rhs_len - 9) / col_total)
        QUAL = lhs_QUAL + rhs_QUAL
        
        # info
        AN *= 2
        info = ("AC="+str(AC)+";AN="+str(AN)+";SF=0,1",)        
        
        #return row[:5]+(QUAL,)+(row[6],)+info+(row[8],)+row[9:lhs_len]+row[lhs_len + 9:]
        return row[:5]+(QUAL,)+(row[6],)+info+(":".join(dup_format), )+result_lhs + result_rhs