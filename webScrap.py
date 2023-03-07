from bs4 import BeautifulSoup
import requests
import time
from datetime import date, datetime
import re
import mysql.connector
import zipfile
import os
import shutil
import threading
import schedule
import logging
import mail


class mlhTracker:
    def __init__(self):
        self.timestr = time.strftime("%Y%m%d-%H%M%S")
        self.today = str(datetime.today()).split()[0]
        self.current_time = datetime.now().time()
        self.logingTime = time.strftime("%Y/%m/%d %H:%M:%S")
        logging.basicConfig(filename='error.log', level=logging.ERROR)

    def dbConnection(self):
        try:
            # database configuration
            connection = mysql.connector.connect(
                host="162.241.85.86",
                user="mlhtracc_localhost",
                password="MLHTracker@123",
                database="mlhtracc_tracker"
            )
        except Exception as e:
            logging.error("Error in Database connection"+self.logingTime + str(e))
        return connection

    def checkTheFoldersIsExpired(self):
        directories = os.listdir()
        date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
        date_directories = [directory for directory in directories if date_pattern.match(directory)]

        for folder in date_directories:
            folderPath = folder
            folderDate = datetime.strptime(folder, '%Y-%m-%d')
            currentDate = datetime.now()
            daysDiff = (currentDate - folderDate).days

            if daysDiff > 60:
                shutil.rmtree(folderPath)
            else:
                continue;

    def getRegions(self):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM regions")
            regions = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logging.error("Error in select regions"+self.logingTime + str(e))
        return regions

    def getLenders(self, regionId):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "SELECT * FROM lenders WHERE regionId = %s"
            params = (regionId,)
            cursor.execute(sql, params)
            lenders = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logging.error("Error in select lenders"+self.logingTime + str(e))
        return lenders

    def removeTags(self,html):
        soup = BeautifulSoup(html, "html.parser")

        for data in soup(['style', 'script']):
            # Remove tags
            data.decompose()
        return soup;

    def getRegionLastModifiedDate(self,regionUId):
        URL = 'https://lendershandbook.ukfinance.org.uk/lenders-handbook/' + regionUId + '/'
        # Page content from Website URL
        page = ""
        try:
            page = requests.get(URL)
        except Exception as e:
            logging.error(self.logingTime+str(e))

        soup = ""
        try:
            soup = self.removeTags(page.content)
        except Exception as e:
            logging.error(self.logingTime+str(e))

        publish = soup.find_all(id="publish")

        regionLastModifiedDate = ""

        if len(publish) >= 1:
            regionLastModifiedStr = publish[0].getText()
            regionLastModifiedDateStr = regionLastModifiedStr.split("Last modified: ", 1)[1]
            regionLastModifiedDateStr = regionLastModifiedDateStr.replace(" ", "")
            regionLastModifiedDate = datetime.strptime(regionLastModifiedDateStr, '%d/%m/%Y').date()

        return regionLastModifiedDate

    def getPage(self,regionUId, lenderUId):
        URL = 'https://lendershandbook.ukfinance.org.uk/lenders-handbook/' + regionUId + '/' + lenderUId + '/question-list/'
        # Page content from Website URL
        page = ""
        try:
            page = requests.get(URL)
        except Exception as e:
            logging.error(self.logingTime+str(e))
        return page

    def getContent(self,lenderLastUpdatedDate,soup):
        content = "";
        layouts = soup.find_all("div", class_="qanda")

        for layout in layouts:
            htmlLayout = layout
            layout = layout.getText()
            links = htmlLayout.find_all('a')
            for link in links:
                href = link.get('href')
                new_href = 'https://lendershandbook.ukfinance.org.uk' + href
                link['href'] = new_href
            htmlLayout['style'] = 'background: #c5e6dd; padding: 5px 20px; margin-bottom: 5px; margin-top: 5px'
            htmlLayout = str(htmlLayout).replace("\n", "")
            lenderLastPublishedStr = layout.split("Last updated: ", 1)

            if len(lenderLastPublishedStr) > 1:
                lenderLastPublishedStr = layout.split("Last updated: ", 1)[1]
                match = re.search(r'\d{2}/\d{2}/\d{4}', lenderLastPublishedStr)

                if match:
                    lenderLastPublishedStr = match.group()
                    removeWord = "Last updated: " + str(lenderLastPublishedStr)
                    htmlLayout = str(htmlLayout).replace(removeWord, "")
            content = content + str(htmlLayout)
            content = content + "\n"
        return content;

    def updateRegionDate(self,regionId, regionLastModifiedDate):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "UPDATE regions SET lastUpdatedDate = %s WHERE regionId = %s"
            params = (regionLastModifiedDate, regionId)
            cursor.execute(sql, params)
            connection.commit()
        except Exception as e:
            logging.error(self.logingTime + str(e))
        finally:
            cursor.close()

    def updateLenderDate(self,lenderId, lenderLastModifiedDate):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "UPDATE lenders SET lastUpdatedDate = %s WHERE lenderId = %s"
            params = lenderLastModifiedDate, lenderId
            cursor.execute(sql, params)
            connection.commit()
        except Exception as e:
            logging.error("Error in update lenders updated date"+self.logingTime + str(e))
        finally:
            cursor.close()

    def getCustomers(self):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "SELECT customerId, firstName, lastName, email, phone, address, company, emailValidation from customers where emailValidation = %s"
            params = (1,)
            cursor.execute(sql, params)
            customers = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logging.error("Error in get customers"+self.logingTime + str(e))
        return customers

    def getCustomerLenders(self,customerId):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "SELECT customerId, regions.uId as regionUId, regions.region as region, lenders.uId as lenderUId, " \
                  "lenders.lender as lender, lenders.lenderId FROM lender_child LEFT OUTER JOIN regions ON lender_child.regionId = regions.regionId " \
                  "LEFT OUTER JOIN lenders ON lender_child.lenderId = lenders.lenderId WHERE customerId = %s"
            params = (customerId,)
            cursor.execute(sql, params)
            customerLenders = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logging.error("Error in get customer lenders"+self.logingTime + str(e))
        return customerLenders

    def getLenderLastRunDate(self, UId, zipUId):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "SELECT regionId From regions WHERE uId = %s"
            params = (zipUId,)
            cursor.execute(sql, params)
            regionId = cursor.fetchone()
        except Exception as e:
            logging.error("Error in get last run date"+self.logingTime + str(e))
        for regionId in regionId:
            regionId = regionId

        try:
            sql = "SELECT lastRunDate From lenders WHERE uId = %s AND regionId = %s"
            params = (UId,regionId)
            cursor.execute(sql, params)
            lenderLastRunDate = cursor.fetchone()
            cursor.close()
        except Exception as e:
            logging.error("Error in get last run date"+self.logingTime + str(e))
        for lenderLastRunDate in lenderLastRunDate:
            lenderLastRunDate = lenderLastRunDate
        return lenderLastRunDate

    def updateLenderLastRunDate(self, lastRunDate,lenderId):
        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "UPDATE lenders SET lastRunDate = %s WHERE lenderId = %s"
            params = lastRunDate, lenderId
            cursor.execute(sql, params)
            connection.commit()
            cursor.close()
        except Exception as e:
            logging.error("Error in update lenders last run date"+self.logingTime + str(e))

    def getLastUpdateDate(self,zipFileName, txtFileName):
        UId = txtFileName.split("_")[0]
        zipUId = zipFileName.split("_")[0]

        connection = self.dbConnection()
        cursor = connection.cursor()
        try:
            sql = "SELECT regionId From regions WHERE uId = %s"
            params = (zipUId,)
            cursor.execute(sql, params)
            regionId = cursor.fetchone()
        except Exception as e:
            logging.error("Error in get last run date" + self.logingTime + str(e))
        for regionId in regionId:
            regionId = regionId

        try:
            sql = "SELECT lastUpdatedDate From lenders WHERE uId = %s AND regionId = %s"
            params = (UId, regionId)
            cursor.execute(sql, params)
            lastUpdatedDate = cursor.fetchone()
            cursor.close()
        except Exception as e:
            logging.error("Error in get last run date" + self.logingTime + str(e))
        for lastUpdatedDate in lastUpdatedDate:
            lastUpdatedDate = lastUpdatedDate
        return lastUpdatedDate

    def getOldFile(self,zipFileName, fileName):
        UId = fileName.split("_")[0]
        zipUId = zipFileName.split("_")[0]
        lastRunDate = self.getLenderLastRunDate(UId,zipUId)
        lastRunDateStr = lastRunDate.strftime("%Y%m%d-%H%M%S")

        txtFileName = UId+"_"+lastRunDateStr+".txt"
        zipFileName = zipUId+"_"+lastRunDateStr+".zip"

        oldFiles = list()

        match = re.search(r"\d{4}-\d{2}-\d{2}", str(lastRunDate))

        if match:
            date_str = match.group()
            folderDate = datetime.strptime(date_str, "%Y-%m-%d").date()

            files = os.listdir(str(folderDate))

            if len(files) != 0:
                for file in files:
                    if zipFileName in file:
                        zip_file_path = os.path.join(str(folderDate), file)
                        try:
                            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                                txtFiles = zip_file.namelist()
                                if len(txtFiles) != 0:
                                    for txtFile in txtFiles:
                                        if txtFileName in txtFile:
                                            oldFiles.append(zip_file_path)
                                            oldFiles.append(txtFile)
                        except Exception as e:
                            logging.error(self.logingTime+str(e))
        return oldFiles

    def compareTwoFilesAndGetContent(self, oldFile, newFile):
        oldFileZipPath = oldFile[0]
        newFileZipPath = newFile[0]
        oldTextFile = oldFile[1]
        newTextFile = newFile[1]

        with zipfile.ZipFile(oldFileZipPath, 'r') as zip_file:
            txtFiles = zip_file.namelist()
            if len(txtFiles) != 0:
                for txtFile in txtFiles:
                    if oldTextFile in txtFile:
                        with zip_file.open(oldTextFile) as inner_file:
                            oldFileLines = inner_file.readlines()

        with zipfile.ZipFile(newFileZipPath, 'r') as zip_file:
            txtFiles = zip_file.namelist()
            if len(txtFiles) != 0:
                for txtFile in txtFiles:
                    if newTextFile in txtFile:
                        with zip_file.open(newTextFile) as inner_file:
                            newFileLines = inner_file.readlines()

        content = ""
        for i in range(len(oldFileLines)):
            if oldFileLines[i] != newFileLines[i]:
                content = content + "<div style='background: #c5e6dd; margin-top: 20px;'>"
                content = content + "<h3 style='padding: 10px;'>Previous Wording</h3><div>" + str(oldFileLines[i].decode())
                content = content + "<h3 style='padding: 10px;'>Updated Wording</h3><div>" + str(newFileLines[i].decode())
                content = content + "</div><hr>"

        return content

    def lenderReadAndStore(self):
        regions = self.getRegions()
        folder_path = self.today
        if len(regions) != 0:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            lendersIdSet = set()
            for region in regions:
                regionId = region[0]
                regionUId = region[2]
                regionLastUpdatedDate = datetime.strptime(str(region[3]), '%Y-%m-%d %H:%M:%S').date()

                regionLastModifiedDate = self.getRegionLastModifiedDate(regionUId)

                if regionLastModifiedDate == "":
                    regionLastModifiedDate = regionLastUpdatedDate

                # check region database get updated value and webpage modified value if its change less than modified date it will update
                if regionLastUpdatedDate < regionLastModifiedDate:
                    regionLastModifiedDate = datetime.combine(regionLastModifiedDate, self.current_time)
                    self.updateRegionDate(regionId, regionLastModifiedDate)

                    # create zip file and store that region lenders txt file
                zip_file_path = os.path.join(folder_path, regionUId + '_' + self.timestr + '.zip')
                with zipfile.ZipFile(zip_file_path, 'w') as myzip:
                    lenders = self.getLenders(regionId)
                    if len(lenders) != 0:
                        for lender in lenders:
                            lenderId = lender[0]
                            lenderUId = lender[3]
                            lenderLastUpdatedDate = datetime.strptime(str(lender[4]), '%Y-%m-%d %H:%M:%S').date()
                            page = self.getPage(regionUId, lenderUId)
                            try:
                                soup = self.removeTags(page.content)
                            except Exception as e:
                                logging.error(self.logingTime+str(e))

                            publish = soup.find_all(id="publish")

                            lenderLastModifiedStr = publish[1].getText()
                            lenderLastModifiedDateStr = lenderLastModifiedStr.split("Last modified: ", 1)[1]
                            lenderLastModifiedDateStr = lenderLastModifiedDateStr.replace(" ", "")
                            isDate = re.search(r'\d{2}/\d{2}/\d{4}', lenderLastModifiedDateStr)
                            if isDate == None:
                                continue
                            lenderLastModifiedDate = datetime.strptime(lenderLastModifiedDateStr, '%d/%m/%Y').date()

                            # check lender database get updated value and webpage modified value if its change less than modified date it will update
                            if lenderLastUpdatedDate < lenderLastModifiedDate:
                                lendersIdSet.add(lenderId)
                                lenderLastModifiedDate = datetime.combine(lenderLastModifiedDate, self.current_time)
                                self.updateLenderDate(lenderId, lenderLastModifiedDate)

                                content = self.getContent(lenderLastUpdatedDate, soup)

                                if len(content) != 0:
                                    myzip.writestr(lenderUId + '_' + self.timestr + '.txt', content)
        return lendersIdSet
    def lenderReadAndSendMail(self):
        logging.error(self.logingTime + ": Program Started")

        #self.checkTheFoldersIsExpired()
        folder_path = self.today
        lendersIdSet = self.lenderReadAndStore()

        if os.path.exists(folder_path):
            customers = self.getCustomers()
            if len(customers) != 0:
                for customer in customers:
                    customerId = customer[0]
                    emailId = customer[3]
                    customerLenders = self.getCustomerLenders(customerId)

                    for customerLender in customerLenders:
                        regionUId = customerLender[1]
                        region = customerLender[2]
                        lenderUId = customerLender[3]
                        lender = customerLender[4]
                        zipFileName = regionUId + '_' + self.timestr + '.zip'
                        txtFileName = lenderUId + '_' + self.timestr + '.txt'
                        link = '<a href="https://lendershandbook.ukfinance.org.uk/lenders-handbook/' + regionUId + '/' + lenderUId + '/question-list/">For reference please click this link</a>'

                        content = ""
                        newFile = list()
                        files = os.listdir(self.today)

                        if len(files) != 0:
                            content = content + "<h4>Region :- " + region + "</h4>"
                            content = content + "<h4>Lender :- " + lender + "</h4>"

                            for file in files:
                                if zipFileName in file:
                                    zip_file_path = os.path.join(self.today, file)
                                    try:
                                        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                                            txtFiles = zip_file.namelist()

                                            if len(txtFiles) != 0:
                                                for txtFile in txtFiles:
                                                    if txtFileName in txtFile:
                                                        oldFile = self.getOldFile(zipFileName, txtFileName)
                                                        lastUpdatedDate = self.getLastUpdateDate(zipFileName,
                                                                                                 txtFileName)
                                                        match = re.search(r"\d{4}-\d{2}-\d{2}", str(lastUpdatedDate))

                                                        if match:
                                                            date_str = match.group()
                                                            lastUpdatedDate = datetime.strptime(date_str,
                                                                                                "%Y-%m-%d").date()
                                                        newFile.append(zip_file_path)
                                                        newFile.append(txtFile)

                                                        content = content + "Last updated : " + str(lastUpdatedDate)
                                                        changes = self.compareTwoFilesAndGetContent(oldFile,newFile)

                                                        if changes != "":
                                                            content = content + changes
                                                            content = str(content+link)
                                                            mail.sendHtmlMail(emailId,"MLH Tracker Updates: " + region + " - " + lender + "",content)
                                    except Exception as e:
                                        logging.error(self.logingTime+ str(e))
        lastRunDate = datetime.strptime(self.timestr, "%Y%m%d-%H%M%S")
        lastRunDate = lastRunDate.strftime('%Y-%m-%d %H:%M:%S')

        if len(lendersIdSet) != 0:
            for lenderId in lendersIdSet:
                self.updateLenderLastRunDate(lastRunDate, lenderId)
        logging.error(self.logingTime + "Process Completed")

    def runProgram(self):
        mlhObj = mlhTracker()
        mlhObj.lenderReadAndSendMail()

    def processCheckAll(self,count):
        try:
            schedule.every(4).hours.do(self.runProgram)
            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            if count > 3:
                count = 1
                logging.error(self.logingTime+ "Error in schedule_thread:", str(e))
            else:
                logging.error(self.logingTime+ "Error in schedule_thread:", str(e))
                count = count + 1
                time.sleep(1)
                self.processCheckAll()


count = 1;
mlhObj = mlhTracker()
mlhObj.lenderReadAndSendMail()
thread = threading.Thread(target=mlhObj.processCheckAll, args=(count,))
thread.start()
