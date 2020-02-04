from bs4 import BeautifulSoup
import numpy as np
import requests
import pypyodbc
import os


class getZimTrackingInfor:
    # initialization the parameters and connection the Database
    def __init__(self, db_name, password=""):
        if not os.path.exists(db_name):
            pypyodbc.win_create_mdb(db_name)
        self.db_name = db_name
        self.password = password
        self.connectDB()

    # Information about connection of Database
    def connectDB(self):
        driver = 'Driver={Microsoft Access Driver (*.mdb,*.accdb)};PWD=' + self.password + ";DBQ=" + self.db_name
        try:
            self.conn = pypyodbc.win_connect_mdb(driver)
            return True
        except:
            print("connection is false")
        return False

    # After connection, return the cursor for execution of database
    def getCursor(self):
        try:
            return self.conn.cursor()
        except:
            return

    # The SQL query statement and return all the results
    def selectDB(self, cursor, sql):
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except:
            return []

    # The SQL insert statement and add the result into DB
    def insertDB(self, cursor, sql):
        try:
            cursor.execute(sql)
            self.conn.commit()
            return True
        except:
            return False

    # The close the connection of DB
    def close(self):
        try:
            self.conn.close()
        except:
            return

    # Get the information of main Item of Tracking information
    def mainItem(self, index, content, tableName):
        InforInSameContainer = "('" + content[index]["data-cont-id"] + "'" + ",";
        for i in range(4):
            InforInSameContainer += "'" + content[index].select('td')[i + 1].text + "'" + ","
        InforInSameContainer += "'" + content[index]["data-cons-id"] + "')"
        sql = "insert into"+tableName+"(containerId,activity,location,dateTrack,vessel,bolnumber) values" + InforInSameContainer
        conn.insertDB(cursor, sql)

    # Get the information of second Item of Tracking information
    def childItem(self, index, parentIndex,content, tableName):
        InforInSameContainer = "('" + content[parentIndex]["data-cont-id"] + "'" + ",";
        counter = 0;
        for i in content[index].select('td'):  # 5 data is one set and first data is empty
            counter = counter + 1
            if counter == 1:
                continue
            InforInSameContainer += "'" + i.text.replace("'", "''") + "'" + ","
            if counter % 5 == 0:
                InforInSameContainer += "'" + content[parentIndex]["data-cons-id"] + "')"
                sql = "insert into "+tableName+"(containerId,activity,location,dateTrack,vessel,bolnumber) values" + InforInSameContainer
                conn.insertDB(cursor, sql)
                counter = 0;
                InforInSameContainer = "('" + content[parentIndex]["data-cont-id"] + "'" + ",";

    # Get all information of one BOL number
    def crawlByBOLNum(self, bolNum, tableName):
        page = 'https://www.zim.com/tools/track-a-shipment?consnumber='+bolNum
        respone =requests.get(page)
        respone.encoding = 'utf-8'
        soup = BeautifulSoup(respone.text, 'lxml')
        content = soup.select('.routing-details table')
        parentIndex = 1
        for i in range(1, len(content)):
            if i % 2 != 0:
                parentIndex = i
                conn.mainItem(i, content, tableName)
            else:
                conn.childItem(i, parentIndex, content, tableName)

if __name__ == '__main__':

    path = os.path.join("D://", "zimTrack.mdb")
    tableName = "zImTrackInfor"
    conn = getZimTrackingInfor(path, "")
    cursor = conn.getCursor()
    # get the BOL number by accessing the csv file
    data = []
    with open('Master_BOL.CSV') as fileReader:
        line = fileReader.readline()
        while line:
            data.append(line)
            line = fileReader.readline()
    data = np.array(data)   # Transforming the data from list to array
    counter = 0             # used for skip the no means information in CSV like title:Master BL#
    for oblNum in data:
        if counter != 0:
            conn.crawlByBOLNum(oblNum.replace("\n",""),tableName)  # deleting "line break"
        counter = 1
    # get all information in DB
    sql = "SELECT * from "+tableName
    rows = cursor.execute(sql)
    for item in rows:
        print(item)
    # close the connection
    cursor.close()
    conn.close()
