import yaml
from SPARQLWrapper import SPARQLWrapper, POST, BASIC
from pykg2tbl import DefaultSparqlBuilder, KGSource
from pathlib import Path
from pandas import DataFrame
from threading import Thread
from time import sleep


CONF_FILE_PATH = 'FAIR-EASE-harvester/config.yaml'
DEFAULT_LATEST_HARVESTED_DATE = '1900-01-01T00:00:00Z'

class harvester :
    sourceIdx           : int = 0
    harvestEveryNSecond : int = 10
    TSusername          : str = ''
    TSpassword          : str = ''
    TShost              : str = ''
    distantID           : str = ''
    distantBaseUrl      : str = ''
    distantName         : str = ''
    distantContactPoint : str = ''
    distantType         : str = ''
    distantUsername     : str = ''
    distantPassword     : str = ''
    distantLatestDate   : str = ''
    adminNode           : str = ''
    GENERATOR = DefaultSparqlBuilder(templates_folder=str(Path().absolute())+'/FAIR-EASE-harvester/templatedQueries/')


    def __init__(self, sourceIdx: int = 0):
        print('Thread start for source : {0}'.format(sourceIdx))
        self.sourceIdx = sourceIdx
        self.loadLocalConfig()
        while True:
            self.loadSourceConfig()
            self.findNewNode()
            sleep(self.harvestEveryNSecond)


    def loadLocalConfig(self):
        with open(CONF_FILE_PATH, 'r') as f:
            valConf = yaml.safe_load(f)
            if 'harvester_config' in valConf and 'triplestore' in valConf['harvester_config']:
                self.TShost = valConf['harvester_config']['triplestore']['baseUrl']

                if 'datasetname' in valConf['harvester_config']['triplestore']:
                    self.TSdatasetName = valConf['harvester_config']['triplestore']['datasetname']

                endpoint = self.TShost + ('/' if self.TShost[-1] != '/' else '' ) + self.TSdatasetName
                self.sparql = SPARQLWrapper(endpoint=endpoint, updateEndpoint=endpoint+'/update')
                self.adminNode = 'http://fair-ease/admin#node'

                if 'username' in valConf['harvester_config']['triplestore']:
                    self.TSusername = valConf['harvester_config']['triplestore']['username']
                if 'password' in valConf['harvester_config']['triplestore']:
                    self.TSpassword = valConf['harvester_config']['triplestore']['password']
                    self.sparql.setHTTPAuth(BASIC)
                    self.sparql.setCredentials(self.TSusername, self.TSpassword)


    def loadSourceConfig(self):
        with open(CONF_FILE_PATH, 'r') as f:
            valConf = yaml.safe_load(f)
            if 'sources' not in valConf:
                return
            if len(valConf) == 0:
                return

            self.distantID =           valConf['sources'][self.sourceIdx]['id']
            self.distantBaseUrl =      valConf['sources'][self.sourceIdx]['baseurl']
            self.distantName =         valConf['sources'][self.sourceIdx]['name']
            self.distantContactPoint = valConf['sources'][self.sourceIdx]['contactPoint']
            self.distantType =         valConf['sources'][self.sourceIdx]['type']

            if 'harvestEveryNSecond' in valConf['sources'][self.sourceIdx]:
                self.harvestEveryNSecond = valConf['sources'][self.sourceIdx]['harvestEveryNSecond']

            if 'username' in valConf['sources'][self.sourceIdx]:
                self.distantUsername = valConf['sources'][self.sourceIdx]['username']
            if 'password' in valConf['sources'][self.sourceIdx]:
                self.distantPassword = valConf['sources'][self.sourceIdx]['password']


    def getResult(self, queryFileName: str, KG : KGSource, **vars) -> DataFrame:
        sparql = self.GENERATOR.build_syntax(queryFileName, **vars)
        test = KG.query(sparql=sparql).to_dataframe()
        return test


    # can't insert/delete RDF with the pykg2tbl (yet ?), doing simple POST request
    def insertDeleteResult(self, queryFileName: str, **vars):
        sparqlWrapper = SPARQLWrapper(endpoint=self.TShost, updateEndpoint=self.TShost + '/update')
        sparqlWrapper.setHTTPAuth(BASIC)
        sparqlWrapper.setCredentials(self.TSusername, self.TSpassword)
        sparqlWrapper.setMethod(POST)

        sparql = self.GENERATOR.build_syntax(queryFileName, **vars)
        sparqlWrapper.setQuery(sparql)

        sparqlWrapper.query()


    def findNewNode(self) -> None:
        catalogContent: KGSource = KGSource.build(self.distantBaseUrl)
        catalogURL = self.getResult('findSubCatalogURL.sparql', catalogContent)

        tripleStoreSource: KGSource = KGSource.build(self.TShost)
        latestUpdateDate = self.getResult(queryFileName='findLatestUpdateDate.sparql', KG=tripleStoreSource, adminNode=self.adminNode, baseUrl=self.distantBaseUrl)

        if 'subCatalogURL' in catalogURL and len(catalogURL['subCatalogURL']) == 0:
            print('{0}\t| Error, no subCatalog found'.format(self.distantBaseUrl))
            return

        if len(latestUpdateDate) > 0:
            distantLatestDate = latestUpdateDate['date'][0]
        else :
            distantLatestDate = DEFAULT_LATEST_HARVESTED_DATE

        for nSubCatalog in range(0, len(catalogURL['subCatalogURL'])):
            subCatalogURL = catalogURL['subCatalogURL'][nSubCatalog]
            subCatalog: KGSource = KGSource.build(subCatalogURL)
            newestDatasetURL = self.getResult(queryFileName='findNewestDatasetURL.sparql', KG=subCatalog, latestHarvested=distantLatestDate)

            if 'datasetURL' not in newestDatasetURL or len(newestDatasetURL['datasetURL']) == 0:
                print('{0}\t| no new Dataset Found'.format(self.distantBaseUrl))
                return

            for nDataset in range(0, len(newestDatasetURL['datasetURL'])):
                datasetUrl = newestDatasetURL['datasetURL'][nDataset]

                graphExist = self.getResult(queryFileName='checkGraphExistence.sparql', KG=tripleStoreSource, datasetURL=datasetUrl)
                if 's' in graphExist :
                    print('{0}\t| dataset already exist, delete it'.format(self.distantBaseUrl))
                    self.insertDeleteResult(queryFileName='deleteExistingGraph.sparql', datasetURL=datasetUrl)

                datasetContent = self.getResult(queryFileName='getCompleteDataset.sparql', KG=subCatalog, datasetURL=datasetUrl)
                self.insertDeleteResult(queryFileName='insertDataset.sparql', datasetURL=datasetUrl, datasetContent=datasetContent)
                print('{0}\t| Insert dataset : {1} '.format(self.distantBaseUrl, datasetUrl))

                if nDataset == 0 and nSubCatalog == 0:
                    date = newestDatasetURL['modifiedDate'][0]
                    print('{0}\t| --------- Set as Newest Date : {0} ---------'.format(self.distantBaseUrl, date))
                    self.insertDeleteResult(queryFileName='deleteOldNewestDate.sparql', baseUrl=self.distantBaseUrl, adminNode=self.adminNode)
                    self.insertDeleteResult(queryFileName='insertNewestDate.sparql', baseUrl=self.distantBaseUrl, newDate=date, adminNode=self.adminNode)


if __name__ == '__main__':
    nbSource = 0
    with open(CONF_FILE_PATH, 'r') as f:
        valConf = yaml.safe_load(f)

        if 'sources' not in valConf:
            print('Error no sources in valConf')
            exit()
        nbSource = len(valConf['sources'])

    for i in range(0, nbSource):
        harv = Thread(target=harvester, args=[i])
        harv.run()
