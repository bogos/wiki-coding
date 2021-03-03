
import concurrent.futures
import requests as req
from datetime import datetime, timedelta
import gzip
import os
import pandas as pd
import wget

class WikiGzFiles():
    def __init__(self, save_file_path = './data'):
        self.save_file_path = "./data"

    def createUrlPath(self, year, month, day, hour):
        return f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz"
        
    def downloadGzFile(self, date_array):
        year, month, day, hour = date_array
        url = self.createUrlPath(year, month, day, hour)
        print("URL", url)
        try:
            response = req.get(url, allow_redirects=True)
            fileName = f"pageviews-{year}{month}{day}-{hour}0000"
            # open(f"{fileName}.gz", "wb").write(response.content)
            wget.download(url, f"{fileName}.gz", bar=None)
            print(f"DONE The file {fileName}.gz is downloaded and saved")
            return fileName
        except Exception as e:
            print(e)
            return e    

    def extractGzFile(self, fileName):
        try:
            with gzip.open(f"{fileName}.gz", 'rb') as f:
                with open(f"{self.save_file_path}/{fileName}", 'wb') as fOut:
                    fileContent = f.read()
                    fOut.write(fileContent)
                    return f"EXTRACTED file in {self.save_file_path}/{fileName}"
        except Exception as e:
            print(e)
            return e
    
    def downloadThread(self, date_array):
        fileNames = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(self.downloadGzFile, x) for x in date_array]

        for f in concurrent.futures.as_completed(results):
            fileNames.append(f.result())
        return fileNames

    def extractThread(self, files):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(self.extractGzFile, x) for x in files]

        for f in concurrent.futures.as_completed(results):
            print(f.result())

class LastDateFormat():            
    def getLastDateFormat(self, hour):
        lastDate = datetime.now() - timedelta(hours = hour)
        format = lastDate.strftime("%Y,%m,%d,%H").split(",")
        return format

    def getLastNHoursDateFormat(self, numHours):
        hours = []
        for hour in range(numHours):
            hours.append(LastDateFormat.getLastDateFormat(self, hour))
        return hours

class WikiFilesAnalysis():
    def __init__(self, folder_path = './data'):
        self.folder_path = "./data"

    def getFilesFromFolder(self,path):
        return sorted(os.listdir(path), key=self.last6Chars)

    def last6Chars(self, x):
        return(x[-6:])

    def languageDomain(self, fileName):
        try:
            df = pd.read_csv(f"{self.folder_path}/{fileName}", sep=" ", names=['domain_code','page_title', 'view_count', 'size_bytes'])
            language, domain, viewCount, _ = df.iloc[df['view_count'].idxmax()]
            hourFormat  = self.hourToIp(fileName)
            return f"{hourFormat}\t {language}\t\t {domain}\t {viewCount}"
        except Exception as e:
            print(e)
            return e

    def hourToIp(self,fileName):
        hour = fileName.split("-")[2][:2]
        return datetime.strptime(f"{hour}:00", "%H:%M").strftime("%I%p")

    def languageDomainThread(self, folder_path):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            files = self.getFilesFromFolder(self.folder_path)
            results = executor.map(self.languageDomain, files)

        for result in results:
            print(result)

if __name__ == '__main__':
    
    # Get Date
    numberDates = LastDateFormat().getLastNHoursDateFormat(5)

    # Download Files
    wikiFiles = WikiGzFiles()
    fileName = wikiFiles.downloadThread(numberDates)
    wikiFiles.extractThread(fileName)

    # Analyze files
    print("Period\tLanguage\tDomain\t\tViewCount")
    WikiFilesAnalysis().languageDomainThread('./data')
