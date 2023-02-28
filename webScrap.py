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
        logging.basicConfig(filename='error.log', level=logging.ERROR)
        # database configuration
        self.connection = mysql.connector.connect(
            host="162.241.85.86",
            user="mlhtracc_localhost",
            password="MLHTracker@123",
            database="mlhtracc_tracker"
        )

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
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM regions")
        regions = cursor.fetchall()
        cursor.close()
        return regions

    def getLenders(self, regionId):
        cursor = self.connection.cursor()
        sql = "SELECT * FROM lenders WHERE regionId = %s"
        params = (regionId,)
        cursor.execute(sql, params)
        lenders = cursor.fetchall()
        cursor.close()
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
        try:
            page = requests.get(URL)
        except Exception as e:
            logging.error(str(e))
        try:
            soup = self.removeTags(page.content)
        except Exception as e:
            logging.error(str(e))

        publish = soup.find_all(id="publish")

        regionLastModifiedStr = publish[0].getText()
        regionLastModifiedDateStr = regionLastModifiedStr.split("Last modified: ", 1)[1]
        regionLastModifiedDateStr = regionLastModifiedDateStr.replace(" ", "")
        regionLastModifiedDate = datetime.strptime(regionLastModifiedDateStr, '%d/%m/%Y').date()
        return regionLastModifiedDate

    def getPage(self,regionUId, lenderUId):
        URL = 'https://lendershandbook.ukfinance.org.uk/lenders-handbook/' + regionUId + '/' + lenderUId + '/question-list/'
        # Page content from Website URL
        try:
            page = requests.get(URL)
        except Exception as e:
            logging.error(str(e))
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
        cursor = self.connection.cursor()
        sql = "UPDATE regions SET lastUpdatedDate = %s WHERE regionId = %s"
        params = (regionLastModifiedDate, regionId)
        cursor.execute(sql, params)
        connection.commit()
        cursor.close()

    def updateLenderDate(self,lenderId, lenderLastModifiedDate):
        cursor = self.connection.cursor()
        sql = "UPDATE lenders SET lastUpdatedDate = %s WHERE lenderId = %s"
        params = lenderLastModifiedDate, lenderId
        cursor.execute(sql, params)
        connection.commit()
        cursor.close()

    def getCustomers(self):
        cursor = self.connection.cursor()
        sql = "SELECT customerId, firstName, lastName, email, phone, address, company, emailValidation from customers where emailValidation = %s"
        params = (1,)
        cursor.execute(sql, params)
        customers = cursor.fetchall()
        cursor.close()
        return customers

    def getCustomerLenders(self,customerId):
        cursor = self.connection.cursor()
        sql = "SELECT customerId, regions.uId as regionUId, regions.region as region, lenders.uId as lenderUId, " \
              "lenders.lender as lender FROM lender_child LEFT OUTER JOIN regions ON lender_child.regionId = regions.regionId " \
              "LEFT OUTER JOIN lenders ON lender_child.lenderId = lenders.lenderId WHERE customerId = %s"
        params = (customerId,)
        cursor.execute(sql, params)
        customerLenders = cursor.fetchall()
        cursor.close()
        return customerLenders

    def lenderReadAndStore(self):
        regions = self.getRegions()

        folder_path = self.today
        if len(regions) != 0:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            for region in regions:
                regionId = region[0]
                regionUId = region[2]
                regionLastUpdatedDate = datetime.strptime(str(region[3]), '%Y-%m-%d %H:%M:%S').date()

                regionLastModifiedDate = self.getRegionLastModifiedDate(regionUId)

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
                                logging.error(str(e))

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
                                lenderLastModifiedDate = datetime.combine(lenderLastModifiedDate, self.current_time)
                                self.updateLenderDate(lenderId, lenderLastModifiedDate)

                                content = self.getContent(lenderLastUpdatedDate, soup)

                                if len(content) != 0:
                                    myzip.writestr(lenderUId + '_' + self.timestr + '.txt', content)

    def lenderReadAndSendMail(self):
        logingTime = time.strftime("%Y/%m/%d %H:%M:%S")
        logging.error(logingTime + ": Program Started")

        #self.checkTheFoldersIsExpired()
        folder_path = self.today
        self.lenderReadAndStore()

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
                                                        try:
                                                            with zip_file.open(txtFile) as inner_file:
                                                                text = inner_file.read()
                                                                text = text.decode()
                                                                content = content + text
                                                        except Exception as e:
                                                            logging.error(str(e))
                                                        content = content+link

                                                        mail.sendHtmlMail(emailId,"MLH Tracker Updates: " + region + " - " + lender + "",content)

                                                        exit()
                                    except Exception as e:
                                        logging.error(str(e))
    def processCheckAll(self,count):
        try:
            schedule.every(4).hours.do(self.lenderReadAndSendMail)
            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            if count > 3:
                count = 1
                logging.error("Error in schedule_thread:", str(e))
            else:
                logging.error("Error in schedule_thread:", str(e))
                count = count + 1
                time.sleep(1)
                self.processCheckAll()


count = 1;
mlhObj = mlhTracker()
mlhObj.lenderReadAndSendMail()
thread = threading.Thread(target=mlhObj.processCheckAll, args=(count,))
thread.start()
