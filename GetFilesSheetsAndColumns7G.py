from pandas import DataFrame, read_csv
import pandas as pd
import xlrd
import datetime
import os
import re
import sqlite3
import CySolsOpsDBLib

CySolOPsDBName           = ""
oFile                    = ""
dFile                    = ""
xPath                    = ""
oPath                    = ""
lFile                    = ""
eFile                    = ""
fList                    = ""
types                    = {}
serial                   = 0
SUPPDateCols             = (14,19,20,22)
CustomerMasterDateCols   = ()
ProductMasterDateCols    = ()
InventoryDateCols        = ()


def Init():    
    global oFile,dFile, xPath, oPath, lFile, eFile, fList, CySolOPsDBName
    pDir           = os.path.dirname (__file__)
    prog           = os.path.basename(__file__)
    dDir           = os.path.join(pDir, "../data/")
    dDir           = os.path.normpath(dDir)
    oFile          = os.path.join(dDir, "CysolOpsColName.csv")
    dFile          = os.path.join(dDir, "CysolOpsSupData.csv")
    lFile          = os.path.join(dDir, "GetFilesSheetsAndColumnsG.log")
    eFile          = os.path.join(dDir, "GetFilesSheetsAndColumnsG.err")
    fList          = os.path.join(dDir, "ListofFiles.log")
    #xPath         = os.path.join(dDir, "CySolsOpsT")
    xPath          = os.path.normpath("//css-warehouse/Support/CySolsOpsToSoumu/CySolsOpsInv/CySolsOps/")
    CySolOPsDBName = os.path.join(dDir,"CYSOLSOPS.db")
    oPath          = os.path.join(dDir, "OFiles")
    if  os.path.exists(oFile): 
        os.remove(oFile)
    if  os.path.exists(lFile): 
        os.remove(lFile)
    if  os.path.exists(eFile): 
        os.remove(eFile)
    if  os.path.exists(dFile): 
        os.remove(dFile)
    if  os.path.exists(fList): 
        os.remove(fList)
    if  os.path.exists(CySolOPsDBName): 
        os.remove(CySolOPsDBName)
    log (lFile, "got oFile = %s, xPath = %s\n" % (oFile, xPath))


def getFileList(xPath):
    global lFile, eFile
    fileList  = os.listdir(xPath)
    nFileList = []
    fileCnt   = 0

    fOut   = open(fList, "w",encoding='utf-8') #sjis
    for fileName in fileList:
        if ((re.search('.*xls$',   fileName) and
            re.search('^Support_',fileName) and
            not re.search('201',  fileName) and 
            not re.search('202',  fileName)) or
            (re.search('^CustomerMaster', fileName) or
             re.search('^ProductMaster', fileName) or
             re.search('\BInventory', fileName))):
            fOut.write(fileName + "\n")
            nFileList.append(fileName)
            fileCnt += 1
    fOut.close()
    log(lFile, "Got %d files from xPath = %s\n" % (fileCnt, xPath))
    return nFileList

def getSupportColumns(fileList, proc, mySRow, myERow):
    fileNo = 0
    sheetNo= 0
    tRows  = 0
    tVRows = 0
    tIRows = 0

    for fileName in fileList:
        #filename = "Support_NK4-CMS.xls"
        sRow   = mySRow
        eRow   = myERow
        if  (re.search('^Support_',fileName)):
            fileNo      += 1
            filePath     = os.path.join (xPath, fileName)
            wb = xlrd.open_workbook(filePath, on_demand=True)
            sheets = len(wb.sheet_names())
            for sheet in wb.sheets():
                sheetName  = sheet.name
                if  (sheet.nrows == 0): continue
                if  (sRow > sheet.nrows):
                     print ("skipping " + fileName + " " + sheetName + " no Rows")
                     continue
                sheetNo   += 1
                myFile = os.path.join(oPath, fileName + "." + sheetName + ".csv")
                fOut   = open(myFile, "w",encoding='utf-8')
                log(lFile, "doing    %20s [%20s] %4d:%2d %s  " % (fileName, sheetName, sheet.nrows, sheet.ncols, proc))
                rows       = 0
                vRows      = 0
                iRows      = 0
                if  (eRow == 0):  eRow = sheet.nrows
                for row in range (sRow, eRow):
                    colDetails = fileName + ',' + sheetName + ','+ str(row)+ ','
                    values = []
                    values.append(fileName)
                    values.append(sheetName)
                    values.append(str(row))
                    rows  += 1
                    validR = 0
                    for col in range(sheet.ncols):
                        (myValue, validC) = GetColValToStr(sheet,row,col)
                        if  (validC):  validR = 1
                        if  (not validC and validR ):  
                             print ("No value at %d %d" % (row,col))
                        else:
                            (myNValue) = NormalizeCellVal(myValue, wb.datemode, row,col)
                            if   (col < 24):                   # Take only 24 cols
                                  values.append(myNValue)
                    valuesGot   = len(values)
                    for col in range(valuesGot,27):
                        values.append("null")
                    colDetails += ','.join(values)
                    colDetails += "\n"
                    if  (validR): 
                        fOut.write(colDetails)
                        (res,msg) = doInsertSupport(values,CySolOPsCursor,CySolOPsConn)
                        if  (res):
                            log (eFile, "Insert failed: %s %s row:%d cols: %d %s\n" % (fileName, sheetName, row, len(values), msg))
                        else:
                            CySolOPsConn.commit()
                            iRows += 1
                        vRows += 1
                        #print ("Got %s %s  row:%2d %2d cols" % (filename, sheetname,row, len(values)))
                    #else: print ("invalid row %d" % row)
                log(lFile, "Done. (%d datarows [%d] inserted %d )\n" % (rows,vRows, iRows))
                fOut.close()
                tRows  += rows
                tVRows += vRows
                tIRows += iRows
            wb.release_resources()
            del wb
            #break
    log(lFile, "Done. Coldetails of All Support Files %s Files:%s sheets (%d [%d] %d datarows)\n" % (fileNo,sheetNo, tRows,tVRows, tIRows))

def getCustomerColumns(fileList, proc, mySRow, myERow):
    fileNo = 0
    sheetNo= 0
    tRows  = 0
    tVRows = 0
    tIRows = 0

    
    for fileName in fileList:
        #filename = "Support_NK4-CMS.xls"
        sRow   = mySRow
        eRow   = myERow
        if  (re.search('^CustomerMaster',fileName)):
                    fileNo      += 1
                    filePath     = os.path.join (xPath, fileName)
                    wb = xlrd.open_workbook(filePath, on_demand=True)
                    sheets = len(wb.sheet_names())
                    for sheet in wb.sheets():
                        sheetName  = sheet.name
                        if  (sheet.nrows == 0): continue
                        if  (sRow > sheet.nrows):
                             print ("skipping " + fileName + " " + sheetName + " no Rows")
                             continue
                        sheetNo   += 1
                        myFile = os.path.join(oPath, fileName + "." + sheetName + ".csv")
                        fOut   = open(myFile, "w",encoding='utf-8')
                        log(lFile, "doing    %20s [%20s] %4d:%2d %s  " % (fileName, sheetName, sheet.nrows, sheet.ncols, proc))
                        rows       = 0
                        vRows      = 0
                        iRows      = 0
                        if  (eRow == 0):  eRow = sheet.nrows
                        for row in range (sRow, eRow):
                            colDetails = fileName + ',' + sheetName + ','+ str(row)+ ','
                            values = []
                            values.append(fileName)
                            values.append(sheetName)
                            values.append(str(row))
                            rows  += 1
                            validR = 0
                            for col in range(sheet.ncols):
                                (myValue, validC) = GetColValToStr(sheet,row,col)
                                if  (validC):  validR = 1
                                if  (not validC and validR ):  
                                     print ("No value at %d %d" % (row,col))
                                else:
                                    (myNValue) = NormalizeCellVal(myValue, wb.datemode, row,col)
                                    if   (col < 17):                   # Take only 17 cols
                                          values.append(myNValue)
                            valuesGot   = len(values)
                            for col in range(valuesGot,20):
                                values.append("null")
                            colDetails += ','.join(values)
                            colDetails += "\n"
                            if  (validR): 
                                fOut.write(colDetails)
                                (res,msg) = doInsertCustomer(values,CySolOPsCursor,CySolOPsConn)
                                if  (res):
                                    log (eFile, "Insert failed: %s %s row:%d cols: %d %s\n" % (fileName, sheetName, row, len(values), msg))
                                else:
                                    CySolOPsConn.commit()
                                    iRows += 1
                                vRows += 1
                                #print ("Got %s %s  row:%2d %2d cols" % (filename, sheetname,row, len(values)))
                            #else: print ("invalid row %d" % row)
                        log(lFile, "Done. (%d datarows [%d] inserted %d )\n" % (rows,vRows, iRows))
                        fOut.close()
                        tRows  += rows
                        tVRows += vRows
                        tIRows += iRows
                    wb.release_resources()
                    del wb
                #break
    log(lFile, "Done. Coldetails of All CustomerMaster Files %s Files:%s sheets (%d [%d] %d datarows)\n" % (fileNo,sheetNo, tRows,tVRows, tIRows))

def getProductColumns(fileList, proc, mySRow, myERow):
    fileNo = 0
    sheetNo= 0
    tRows  = 0
    tVRows = 0
    tIRows = 0

    
    for fileName in fileList:
        #filename = "Support_NK4-CMS.xls"
        sRow   = mySRow
        eRow   = myERow
        if  (re.search('^ProductMaster',fileName)):
                    fileNo      += 1
                    filePath     = os.path.join (xPath, fileName)
                    wb = xlrd.open_workbook(filePath, on_demand=True)
                    sheets = len(wb.sheet_names())
                    for sheet in wb.sheets():
                        sheetName  = sheet.name
                        if  (sheet.nrows == 0): continue
                        if  (sRow > sheet.nrows):
                             print ("skipping " + fileName + " " + sheetName + " no Rows")
                             continue
                        sheetNo   += 1
                        myFile = os.path.join(oPath, fileName + "." + sheetName + ".csv")
                        fOut   = open(myFile, "w",encoding='utf-8')
                        log(lFile, "doing    %20s [%20s] %4d:%2d %s  " % (fileName, sheetName, sheet.nrows, sheet.ncols, proc))
                        rows       = 0
                        vRows      = 0
                        iRows      = 0
                        if  (eRow == 0):  eRow = sheet.nrows
                        for row in range (sRow, eRow):
                            colDetails = fileName + ',' + sheetName + ','+ str(row)+ ','
                            values = []
                            values.append(fileName)
                            values.append(sheetName)
                            values.append(str(row))
                            rows  += 1
                            validR = 0
                            for col in range(sheet.ncols):
                                (myValue, validC) = GetColValToStr(sheet,row,col)
                                if  (validC):  validR = 1
                                if  (not validC and validR ):  
                                     print ("No value at %d %d" % (row,col))
                                else:
                                    (myNValue) = NormalizeCellVal(myValue, wb.datemode, row,col)
                                    if   (col < 3):                   # Take only 17 cols
                                          values.append(myNValue)
                            valuesGot   = len(values)
                            for col in range(valuesGot,6):
                                values.append("null")
                            colDetails += ','.join(values)
                            colDetails += "\n"
                            if  (validR): 
                                fOut.write(colDetails)
                                (res,msg) = doInsertProduct(values,CySolOPsCursor,CySolOPsConn)
                                if  (res):
                                    log (eFile, "Insert failed: %s %s row:%d cols: %d %s\n" % (fileName, sheetName, row, len(values), msg))
                                else:
                                    CySolOPsConn.commit()
                                    iRows += 1
                                vRows += 1
                                #print ("Got %s %s  row:%2d %2d cols" % (filename, sheetname,row, len(values)))
                            #else: print ("invalid row %d" % row)
                        log(lFile, "Done. (%d datarows [%d] inserted %d )\n" % (rows,vRows, iRows))
                        fOut.close()
                        tRows  += rows
                        tVRows += vRows
                        tIRows += iRows
                    wb.release_resources()
                    del wb
                #break
    log(lFile, "Done. Coldetails of All ProductMaster Files %s Files:%s sheets (%d [%d] %d datarows)\n" % (fileNo,sheetNo, tRows,tVRows, tIRows))

def getInventoryColumns(fileList, proc, mySRow, myERow):
    fileNo = 0
    sheetNo= 0
    tRows  = 0
    tVRows = 0
    tIRows = 0

    
    for fileName in fileList:
        #filename = "Support_NK4-CMS.xls"
        sRow   = mySRow
        eRow   = myERow
        if  (re.search('\BInventory',fileName)):
                    fileNo      += 1
                    filePath     = os.path.join (xPath, fileName)
                    wb = xlrd.open_workbook(filePath, on_demand=True)
                    sheets = len(wb.sheet_names())
                    for sheet in wb.sheets():
                        sheetName  = sheet.name
                        if  (sheet.nrows == 0): continue
                        if  (sRow > sheet.nrows):
                             print ("skipping " + fileName + " " + sheetName + " no Rows")
                             continue
                        sheetNo   += 1
                        myFile = os.path.join(oPath, fileName + "." + sheetName + ".csv")
                        fOut   = open(myFile, "w",encoding='utf-8')
                        log(lFile, "doing    %20s [%20s] %4d:%2d %s  " % (fileName, sheetName, sheet.nrows, sheet.ncols, proc))
                        rows       = 0
                        vRows      = 0
                        iRows      = 0
                        if  (eRow == 0):  eRow = sheet.nrows
                        for row in range (sRow, eRow):
                            colDetails = fileName + ',' + sheetName + ','+ str(row)+ ','
                            values = []
                            values.append(fileName)
                            values.append(sheetName)
                            values.append(str(row))
                            rows  += 1
                            validR = 0
                            for col in range(sheet.ncols):
                                (myValue, validC) = GetColValToStr(sheet,row,col)
                                if  (validC):  validR = 1
                                if  (not validC and validR ):  
                                     print ("No value at %d %d" % (row,col))
                                else:
                                    (myNValue) = NormalizeCellVal(myValue, wb.datemode, row,col)
                                    if   (col < 5):                   # Take only 5 cols
                                          values.append(myNValue)
                            valuesGot   = len(values)
                            for col in range(valuesGot,8):
                                values.append("null")
                            colDetails += ','.join(values)
                            colDetails += "\n"
                            if  (validR): 
                                fOut.write(colDetails)
                                (res,msg) = doInsertInventory(values,CySolOPsCursor,CySolOPsConn)
                                if  (res):
                                    log (eFile, "Insert failed: %s %s row:%d cols: %d %s\n" % (fileName, sheetName, row, len(values), msg))
                                else:
                                    CySolOPsConn.commit()
                                    iRows += 1
                                vRows += 1
                                #print ("Got %s %s  row:%2d %2d cols" % (filename, sheetname,row, len(values)))
                            #else: print ("invalid row %d" % row)
                        log(lFile, "Done. (%d datarows [%d] inserted %d )\n" % (rows,vRows, iRows))
                        fOut.close()
                        tRows  += rows
                        tVRows += vRows
                        tIRows += iRows
                    wb.release_resources()
                    del wb
                #break
    log(lFile, "Done. Coldetails of All Inventory Files %s Files:%s sheets (%d [%d] %d datarows)\n" % (fileNo,sheetNo, tRows,tVRows, tIRows))

def GetColValToStr(sheet,row,col):
    (myValue,validC) = ("",0)
    try: 
         myValue     = sheet.cell(row,col).value
         validC      = 1
    except:
         myValue = ""
         log(eFile, "NoVal: %20s %2d:%2d\n" % (sheet.name, row,col))
    return (myValue, validC)
    
def NormalizeCellVal(myCellVal, dateMode, row, col):
    global types, SUPPDateCols, CustomerMasterDateCols,ProductMasterDateCols,InventoryDateCols
    myNValue = ""
    myType   = type(myCellVal)
    if   (not myType in types):
          types[myType]    = 1
    else: 
          types[myType]   += 1
    if   (type(myCellVal) is not str): 
          if  ((col in SUPPDateCols) or (col in CustomerMasterDateCols) ):
               myNValue    = datetime.datetime(*xlrd.xldate_as_tuple(myCellVal, dateMode)).strftime("%Y/%m/%d")
          else:myNValue    = str (myCellVal)
    if   (type(myCellVal) is str): myNValue = myCellVal
    if   (myNValue        == "") : myNValue = "null"
    return (myNValue)    
    
def PrintColumnNamesFromFile(oFile):
    fHand = open(oFile, "r",encoding='utf-8')
    for line in fHand:
        print(line)
    fHand.close()
    
def log(lFile, msg):
    logF = open (lFile, "a",encoding="utf-8")
    logF.write(msg)
    logF.close
    
def doInsertSupport(values,CySolOPsCursor,CySolOPsConn):
    global serial
    suppTuple = tuple(values)
    (res, msg) = CySolsOpsDBLib.sqliteDBInsertSupportTable(CySolOPsCursor, CySolOPsConn, suppTuple)
    if   (res and (values[13] == "null")):
          serial += 1
          values[13] = ("%d" % serial)
          suppTuple  = tuple(values)
          (res, msg) = CySolsOpsDBLib.sqliteDBInsertSupportTable(CySolOPsCursor, CySolOPsConn, suppTuple)
    #print(CySolOPsCursor.rowcount, "record inserted.")
    return (res,msg)

def doInsertCustomer(values,CySolOPsCursor,CySolOPsConn):
    custTuple = tuple(values)
    (res, msg) = CySolsOpsDBLib.sqliteDBInsertCustomerTable(CySolOPsCursor, CySolOPsConn, custTuple)
    return (res, msg)

def doInsertProduct(values,CySolOPsCursor,CySolOPsConn):
    prodTuple = tuple(values)
    (res, msg) = CySolsOpsDBLib.sqliteDBInsertProductTable(CySolOPsCursor, CySolOPsConn, prodTuple)
    return (res, msg)

def doInsertInventory(values,CySolOPsCursor,CySolOPsConn):
    inventoryTuple = tuple(values)
    (res, msg) = CySolsOpsDBLib.sqliteDBInsertInventoryTable(CySolOPsCursor, CySolOPsConn, inventoryTuple)
    return (res, msg)

def checkSupportTuple(CySolOPsCursor):
    global lFile
    CySolOPsCursor.execute("SELECT * FROM support")
    myResult = CySolOPsCursor.fetchall()
    count    = 0
    for x in myResult:
        print(x)
        count += 1
    log (lFile, "Checked %d entries in DB\n"% count)

def checkCustomerTuple(CySolOPsCursor):
    global lFile
    CySolOPsCursor.execute("SELECT * FROM customer")
    myResult = CySolOPsCursor.fetchall()
    count    = 0
    for x in myResult:
        print(x)
        count += 1
    log (lFile, "Checked %d entries in DB\n"% count)

def checkProductTuple(CySolOPsCursor):
    global lFile
    CySolOPsCursor.execute("SELECT * FROM product")
    myResult = CySolOPsCursor.fetchall()
    count    = 0
    for x in myResult:
        print(x)
        count += 1
    log (lFile, "Checked %d entries in DB\n"% count)

def checkInventoryTuple(CySolOPsCursor):
    global lFile
    CySolOPsCursor.execute("SELECT * FROM inventory")
    myResult = CySolOPsCursor.fetchall()
    count    = 0
    for x in myResult:
        print(x)
        count += 1
    log (lFile, "Checked %d entries in DB\n"% count)
    

def showTypes():
    for  type in  types.keys():
         print (type)      
    

Init()
fileList = getFileList(xPath)
(CySolOPsCursor, CySolOPsConn) = CySolsOpsDBLib.sqliteDBOpen (CySolOPsDBName)
CySolsOpsDBLib.sqliteDBCreateSupportTable(CySolOPsCursor)
CySolsOpsDBLib.sqliteDBCreateCustomerTable(CySolOPsCursor)
CySolsOpsDBLib.sqliteDBCreateProductTable(CySolOPsCursor)
CySolsOpsDBLib.sqliteDBCreateInventoryTable(CySolOPsCursor)
#GetColumnNames(fileList,'ColN', oFile, 4, 5)
getSupportColumns(fileList,'Data', 5, 0)
getCustomerColumns(fileList, 'Data', 1, 0)
getProductColumns(fileList, 'Data', 1, 0)
#getInventoryColumns(fileList, 'Data', 1, 10)
#PrintColumnNamesFromFile(oFile)       
checkSupportTuple(CySolOPsCursor)
checkCustomerTuple(CySolOPsCursor)
checkProductTuple(CySolOPsCursor)
checkInventoryTuple(CySolOPsCursor)
CySolOPsConn.close()
showTypes()
    

