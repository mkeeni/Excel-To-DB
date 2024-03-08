import sqlite3


def  sqliteDBOpen (DBName):
     conn = sqlite3.connect(DBName)
     cur  = conn.cursor()
     return (cur, conn)
     
def  sqliteDBCreateSupportTable(cur):
     cur.execute("CREATE TABLE IF NOT EXISTS support(fileName      varchar(50),\
                                                     prodId        varchar(50) NOT NULL,\
                                                     rowNo         integer(5),\
                                                     unnamedData   varchar(5), \
                                                     unnamedDataa  varchar(50),\
                                                     custId        varchar(50) NOT NULL ,\
                                                     agent         varchar(50),\
                                                     distributor   varchar(50),\
                                                     custName      varchar(50),\
                                                     department    varchar(50),\
                                                     title         varchar(50),\
                                                     mainPerson    varchar(50),\
                                                     modelNo       varchar(50),\
                                                     serialNo      varchar(50),\
                                                     accNo         varchar(50),\
                                                     pw            varchar(50),\
                                                     nLicenses     varchar(5),\
                                                     shipDate      varchar(50),\
                                                     useStart      varchar(20),\
                                                     licenseVer    varchar(20),\
                                                     shipVer       varchar(20),\
                                                     contractNo    varchar(50),\
                                                     cStartDate    varchar(20),\
                                                     expDate       varchar(20),\
                                                     comment       varchar(50),\
                                                     unnamedDate   varchar(20),\
                                                     reason        varchar(50),\
                                                     PRIMARY KEY (prodId,custId,modelNo,serialNo))")
def  sqliteDBCreateCustomerTable(cur):
     cur.execute("CREATE TABLE IF NOT EXISTS customer(fileName         varchar(50),\
                                                     sheetName         varchar(50),\
                                                     rowNo             integer(5),\
                                                     custId            varchar(50) NOT NULL ,\
                                                     agent             varchar(50),\
                                                     curDistributor    varchar(50),\
                                                     custName          varchar(50),\
                                                     department        varchar(50),\
                                                     title             varchar(50),\
                                                     mainPerson        varchar(50),\
                                                     mainEmail         varchar(50),\
                                                     tel               varchar(50),\
                                                     fax               varchar(50),\
                                                     postalCode        varchar(50),\
                                                     address           varchar(100),\
                                                     remarks           varchar(50),\
                                                     changes           varchar(50),\
                                                     industry          varchar(50),\
                                                     unnamedColumn1    varchar(50),\
                                                     unnamedColumn2    varchar(50),\
                                                     PRIMARY KEY (custId))")
def  sqliteDBCreateProductTable(cur):
     cur.execute("CREATE TABLE IF NOT EXISTS product(fileName          varchar(50),\
                                                     sheetName         varchar(50),\
                                                     rowNo             integer(5),\
                                                     pCode             varchar(50) NOT NULL ,\
                                                     pname             varchar(50),\
                                                     modelNo           varchar(50),\
                                                     PRIMARY KEY (pCode))")
def  sqliteDBCreateInventoryTable(cur):
     cur.execute("CREATE TABLE IF NOT EXISTS inventory(fileName          varchar(50),\
                                                       sheetName         varchar(50),\
                                                       rowNo             integrr(5) ,\
                                                       seq               integer(5),\
                                                       serial            varchar(50),\
                                                       macAddr           varchar(20),\
                                                       inDate            varchar(20),\
                                                       outDate           varchar(20),\
                                                       PRIMARY KEY (serial))")
                                                       
                                                     
     
                                                     
def sqliteDBInsertSupportTable(cur, conn, Tuple):
         (res, msg) = (0, "")
         sql = "INSERT INTO support (fileName,    prodId,         rowNo,       unnamedData, unnamedDataa, \
                                     custId,         agent,       distributor, custName,       department, \
                                     title,       mainPerson,  modelNo,     serialNo,    accNo, \
                                     pw,          nLicenses,   shipDate,    useStart,    licenseVer, \
                                     shipVer,     contractNo,  cStartDate,  expDate,     comment, \
                                     unnamedDate, reason) \
                                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
         try:
             cur.execute(sql, Tuple)
         except sqlite3.Error as e:
             (prodId,custId, modelNo, serialNo) = (Tuple[1], Tuple[5],Tuple[12],Tuple[13])
             msg = ("sqlite3 insert error. keys: %s,%s,%s,%s " % (prodId,custId, modelNo, serialNo)) + e.args[0]
             res = 1
         return (res, msg)

def sqliteDBInsertCustomerTable(cur, conn, Tuple):
         (res, msg) = (0, "")
         sql = "INSERT INTO customer (fileName,    sheetName,         rowNo,       custId,   agent,\
                                     curDistributor, custName,   department,  title,   mainPerson, \
                                     mainEmail,  tel,     fax,    postalCode,  address, \
                                     remarks,     changes,   industry,    unnamedColumn1, unnamedColumn2 )\
                                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
         try:
             cur.execute(sql, Tuple)
         except sqlite3.Error as e:
             (custId) = (Tuple[3])
             msg = ("sqlite3 insert error. keys: %s " % (custId)) + e.args[0]
             res = 1
         return (res, msg)

def sqliteDBInsertProductTable(cur, conn, Tuple):
        (res, msg) = (0, "")
        sql = "INSERT INTO product (fileName,    sheetName,      rowNo,       pCode,  pName,\
                                     modelNo) \
                                     VALUES (?,?,?,?,?,?)"
        try:
             cur.execute(sql, Tuple)
        except sqlite3.Error as e:
             (pCode) = (Tuple[3])
             msg = ("sqlite3 insert error. keys: %s " % (pCode)) + e.args[0]
             res = 1
        return (res, msg)

def sqliteDBInsertInventoryTable(cur, conn, Tuple):
          (res, msg) = (0, "")
          sql = "INSERT INTO inventory (fileName,    sheetName, rowNo, seq, serial,  macAddr,\
                                     inDate,  outDate) \
                                     VALUES (?,?,?,?,?,?,?,?)"
          try:
             cur.execute(sql, Tuple)
          except sqlite3.Error as e:
             (serial) = (Tuple[4])
             msg = ("sqlite3 insert error. keys: %s " % (serial)) + e.args[0]
             res = 1
          
          return (res, msg)
